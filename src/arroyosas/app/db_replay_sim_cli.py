import asyncio
import logging
import os
import sqlite3
from datetime import datetime

import typer
import zmq
import zmq.asyncio
import msgpack
from tiled.client import from_uri

from ..config import settings
from ..schemas import (
    RawFrameEvent,
    SASStart,
    SASStop,
    SerializableNumpyArrayModel,
)

"""
Simulates image retrieval by reading Tiled URLs from a local SQLite database
and sends the fetched images onto ZMQ.
"""

# Get configuration from environment variables with defaults
DEFAULT_DB_PATH = os.getenv("DB_PATH", "latent_vectors.db")
DEFAULT_API_KEY = os.getenv("TILED_API_KEY", None)

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()


def get_urls_from_db(db_path, limit=None):
    """Get a list of Tiled URLs from the database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        query = "SELECT id, tiled_url FROM vectors ORDER BY id"
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            logger.warning(f"No Tiled URLs found in database {db_path}")
            return []
            
        logger.info(f"Found {len(results)} Tiled URLs in database")
        return results
    except Exception as e:
        logger.error(f"Error reading from database: {e}")
        return []


def read_image_from_tiled_url(tiled_url, api_key=None):
    """
    Read an image from a Tiled URL.
    
    Example URL: http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/79b18ce7-c6a9-4c25-8b7c-7d3c5a57a536/streams/primary/pil2M_image?slice=0:1,0:1679,0:1475
    
    The function:
    1. Extracts the index from the slice parameter (number before ":" in the first part)
    2. Changes "array/full" to "metadata" in the base URL
    3. Extracts the dataset URI (part after "array/full/") without query parameters
    4. Connects to the Tiled server and retrieves the image
    """
    try:
        # Extract index from slice parameter
        index = 0  # Default index
        if '?' in tiled_url and 'slice=' in tiled_url:
            slice_param = tiled_url.split('slice=')[1].split('&')[0]
            if ':' in slice_param:
                parts = slice_param.split(',')[0].split(':')
                if parts[0].isdigit():
                    index = int(parts[0])
        
        # Parse the URL to extract base URL and path
        url_without_query = tiled_url.split('?')[0]  # Remove query parameters
        url_parts = url_without_query.split('/api/v1/')
        
        if len(url_parts) != 2:
            logger.error(f"Invalid Tiled URL format: {tiled_url}")
            return None, 0
        
        # Change array/full to metadata
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
        
        logger.debug(f"Base URI: {base_uri}, Dataset URI: {dataset_uri}, Index: {index}")
        
        # Connect to the Tiled server
        client = from_uri(base_uri, api_key=api_key)
        
        # Access the dataset
        tiled_data = client[dataset_uri]
        logger.debug(f"Dataset shape: {tiled_data.shape}, dtype: {tiled_data.dtype}")
        
        # Retrieve the image at the specified index
        image = tiled_data[index]
        
        return image, index
        
    except Exception as e:
        logger.error(f"Error reading from Tiled URL {tiled_url}: {e}")
        return None, 0


@app.command()
def main(
    db_path: str = typer.Option(DEFAULT_DB_PATH, help="Path to the SQLite database containing Tiled URLs"),
    max_frames: int = typer.Option(10000, help="Maximum number of frames to process"),
    api_key: str = typer.Option(DEFAULT_API_KEY, help="API key for Tiled authentication")
):
    """
    Run the image simulator that reads Tiled URLs from a database, fetches the images, and publishes them via ZMQ.
    
    Configuration can be set via environment variables:
    - DB_PATH: Path to the SQLite database
    - TILED_API_KEY: API key for Tiled authentication
    
    Command-line arguments override environment variables.
    """
    # Log the configuration
    logger.info(f"Starting DB Image Simulator with:")
    logger.info(f"- Database path: {db_path}")
    logger.info(f"- Max frames: {max_frames}")
    logger.info(f"- API key provided: {api_key is not None}")
    
    async def run():
        # Check if database exists
        if not os.path.exists(db_path):
            logger.error(f"Database file not found: {db_path}")
            return

        # Setup ZMQ socket
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.PUB)
        address = settings.tiled_poller.zmq_frame_publisher.address
        logger.info(f"Binding to ZMQ address: {address}")
        socket.bind(address)
        
        # Get URLs from database
        urls = get_urls_from_db(db_path, limit=max_frames)
        if not urls:
            logger.error("No URLs found in database, cannot continue")
            return
            
        # Send start event
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start = SASStart(
            width=1679,    # Default values - these will be updated from real data
            height=1475,
            data_type="uint32",
            tiled_url="db://latent_vectors",
            run_name=f"db_tiled_run",
            run_id=str(current_time),
        )
        logger.info(f"Sending start event")
        await socket.send(msgpack.packb(start.model_dump()))
        
        # Process each URL
        for db_id, tiled_url in urls:
            try:
                logger.info(f"Processing URL from DB record {db_id}: {tiled_url}")
                
                # Read image data from Tiled URL
                image_data, index = read_image_from_tiled_url(tiled_url, api_key)
                
                if image_data is None:
                    logger.error(f"Failed to read image from {tiled_url}")
                    continue
                
                # Send the frame event
                event = RawFrameEvent(
                    image=SerializableNumpyArrayModel(array=image_data),
                    frame_number=index,
                    tiled_url=tiled_url,
                )
                logger.info(f"Sending frame {index}")
                await socket.send(msgpack.packb(event.model_dump()))
                
                # Small delay between frames
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error processing frame from {tiled_url}: {e}")
        
        # Send stop event
        stop = SASStop(num_frames=len(urls))
        logger.info(f"Sending stop event")
        await socket.send(msgpack.packb(stop.model_dump()))
        logger.info(f"Complete - sent {len(urls)} frames")

    asyncio.run(run())


if __name__ == "__main__":
    app()