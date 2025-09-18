import asyncio
import glob
import json
import logging
import os
import re
from datetime import datetime
from typing import List

import typer
import zmq
import zmq.asyncio
import msgpack
import numpy as np
from tiled.client import from_uri

from ..config import settings
from ..schemas import (
    RawFrameEvent,
    SASStart,
    SASStop,
    SerializableNumpyArrayModel,
)

"""
Reads a Tiled URL from a local file and sends images via ZMQ
in the same format as the other simulators.
"""

# Default settings
URL_FILE = os.getenv("URL_FILE", "./tiled_url.json")
TILED_API_KEY = os.getenv("TILED_API_KEY", None)

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()


def load_url_from_file(file_path: str) -> tuple:
    """Load Tiled URL and metadata from a file"""
    try:
        if not os.path.exists(file_path):
            logger.error(f"URL file not found: {file_path}")
            return None, None
            
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        tiled_url = data.get("tiled_url")
        metadata = data.get("metadata", {})
        
        if not tiled_url:
            logger.error(f"No Tiled URL found in {file_path}")
            return None, None
            
        logger.info(f"Loaded Tiled URL from {file_path}: {tiled_url}")
        return tiled_url, metadata
        
    except Exception as e:
        logger.error(f"Error loading URL from file: {e}")
        return None, None


async def fetch_image_from_tiled(client, container_name: str, image_key: str) -> np.ndarray:
    """Fetch a single image from Tiled as a numpy array"""
    try:
        # Get the container
        container = client[container_name]
        
        # Get the image data
        array_client = container[image_key]
        
        # Convert to numpy array
        data = array_client.read()
        
        # Ensure it's a numpy array
        if not isinstance(data, np.ndarray):
            data = np.array(data, dtype=np.uint32)
            
        return data
    except Exception as e:
        logger.error(f"Error fetching image {image_key}: {e}")
        # Return a small dummy array in case of error
        return np.zeros((10, 10), dtype=np.uint32)


async def get_matching_keys(client, container_name: str, pattern: str) -> List[str]:
    """Get all keys in a container matching a pattern"""
    try:
        # Get the container
        container = client[container_name]
        
        # Get all keys
        all_keys = list(container.keys())
        logger.info(f"Found {len(all_keys)} total keys in container")
        
        # Create regex from pattern - properly escape special characters
        regex = pattern
        for char in '[]{}.':
            regex = regex.replace(char, '\\' + char)
        # Replace the digit pattern with actual regex pattern
        regex = regex.replace('\\[0-9\\]\\{4\\}', r'\d{4}')
        regex_pattern = re.compile(regex)
        
        logger.info(f"Using regex pattern: {regex}")
        
        # Filter keys by regex
        matching_keys = sorted([k for k in all_keys if regex_pattern.match(k)])
        logger.info(f"Found {len(matching_keys)} matching image keys")
        
        return matching_keys
    except Exception as e:
        logger.error(f"Error getting matching keys: {e}")
        return []


@app.command()
def main(
    url_file: str = typer.Option(URL_FILE, help="Path to file containing the Tiled URL"),
    api_key: str = typer.Option(TILED_API_KEY, help="API key for Tiled authentication"),
    cycles: int = typer.Option(1, help="Number of cycles to run"),
    pause: float = typer.Option(0.1, help="Pause time between frames"),
    cycle_pause: float = typer.Option(5.0, help="Pause time between cycles"),
):
    """
    Run the image simulator that reads a Tiled URL from a local file,
    fetches the images from Tiled, and publishes them via ZMQ.
    """
    logger.info(f"Starting Local Tiled Simulator with:")
    logger.info(f"- URL file: {url_file}")
    logger.info(f"- Cycles: {cycles}")
    
    async def run():
        # Load Tiled URL from file
        tiled_url, metadata = load_url_from_file(url_file)
        if not tiled_url:
            return
        
        # Parse the URL to extract base URL and path
        url_without_query = tiled_url.split('?')[0]  # Remove query parameters
        url_parts = url_without_query.split('/api/v1/')
        
        if len(url_parts) != 2:
            logger.error(f"Invalid Tiled URL format: {tiled_url}")
            return
        
        # Change array/full to metadata for connection
        base_uri = f"{url_parts[0]}/api/v1/metadata"
        
        # Extract dataset URI - get everything after "array/full/"
        full_path = url_parts[1]
        
        if 'array/full/' in url_without_query:
            # If the URL contains array/full, extract the part after it
            path_parts = full_path.split('array/full/')
            if len(path_parts) > 1:
                dataset_uri = path_parts[1]
            else:
                dataset_uri = full_path
        else:
            # If URL doesn't contain array/full, use the whole path
            dataset_uri = full_path
        
        logger.info(f"Connecting to Tiled server at {base_uri}")
        logger.info(f"Dataset URI: {dataset_uri}")
        
        # Connect to the Tiled server
        client = from_uri(base_uri, api_key=api_key)
        
        # Get container and run_id
        parts = dataset_uri.split('/')
        if len(parts) != 2:
            logger.error(f"Invalid dataset URI format: {dataset_uri}")
            return
            
        container_name = parts[0]
        run_id_base = parts[1]
        
        # If we have a pattern in metadata, use it to find all matching keys
        if metadata and "image_pattern" in metadata:
            pattern = metadata["image_pattern"]
            matching_keys = await get_matching_keys(client, container_name, pattern)
            
            if not matching_keys:
                logger.error(f"No matching keys found for pattern {pattern}")
                return
                
            num_images = len(matching_keys)
        else:
            # Try to get the image count from metadata
            num_images = metadata.get("num_images", 0)
            if num_images <= 0:
                logger.error("No image count found in metadata")
                return
                
            # Generate keys based on the run_id_base
            matching_keys = [f"{run_id_base}_{i:04d}" for i in range(num_images)]
        
        logger.info(f"Found {len(matching_keys)} image keys")
        
        # Get dimensions from metadata
        width = metadata.get("width")
        height = metadata.get("height")
        data_type = metadata.get("data_type")
        
        if not all([width, height, data_type]):
            logger.error("Missing image dimensions in metadata")
            return
        
        # Setup ZMQ socket
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.PUB)
        address = settings.tiled_poller.zmq_frame_publisher.address
        logger.info(f"Binding to ZMQ address: {address}")
        socket.bind(address)
        
        # Run the simulator cycles
        for cycle_num in range(cycles):
            # Get current time formatted as YYYY-MM-DD HH:MM:SS
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Send SAS Start event
            start = SASStart(
                width=width,
                height=height,
                data_type=data_type,
                tiled_url=tiled_url,
                run_name=f"local_tiled_run_{cycle_num}",
                run_id=str(current_time),
            )
            logger.info(f"Sending start event for cycle {cycle_num}")
            await socket.send(msgpack.packb(start.model_dump()))
            
            # Process each frame
            for frame_num, image_key in enumerate(matching_keys):
                try:
                    # Fetch the image from Tiled
                    image_array = await fetch_image_from_tiled(client, container_name, image_key)
                    
                    # Create individual frame URL with the complete path to this specific image
                    # Include slice=0 to explicitly request the first slice
                    frame_url = f"{url_parts[0]}/api/v1/array/full/{container_name}/{image_key}?slice=0:1,0:1679,0:1475"
                    
                    # Create and send the frame event with frame_number always set to 0
                    event = RawFrameEvent(
                        image=SerializableNumpyArrayModel(array=image_array),
                        frame_number=0,  # Always set to 0 since each container has one image
                        tiled_url=frame_url,
                    )
                    logger.info(f"Sending image {image_key} for cycle {cycle_num}")
                    await socket.send(msgpack.packb(event.model_dump()))
                    
                except Exception as e:
                    logger.error(f"Error sending image {image_key}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue
                
                # Pause between frames
                await asyncio.sleep(pause)
            
            # Send stop event
            stop = SASStop(num_frames=len(matching_keys))
            logger.info(f"Sending stop event for cycle {cycle_num}")
            await socket.send(msgpack.packb(stop.model_dump()))
            
            if cycle_num < cycles - 1:
                logger.info(f"Cycle {cycle_num} complete - pausing for {cycle_pause}s")
                await asyncio.sleep(cycle_pause)
        
        logger.info(f"All {cycles} cycles complete")
    
    asyncio.run(run())


if __name__ == "__main__":
    app()