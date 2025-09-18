import asyncio
import glob
import json
import logging
import os
import time
from datetime import datetime
from typing import List

import numpy as np
import typer
from PIL import Image
from tiled.client import from_uri

# Default settings
DEFAULT_IMAGE_FOLDER = os.getenv("DEFAULT_IMAGE_FOLDER", "./images")
TILED_URI = os.getenv("TILED_URI", "http://localhost:8000/api/v1/metadata")
TILED_API_KEY = os.getenv("TILED_API_KEY", None)
TILED_CONTAINER = os.getenv("TILED_CONTAINER", "733data")
URL_FILE = os.getenv("URL_FILE", "./tiled_url.json")

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()


def load_image_files(folder_path: str) -> List[str]:
    """Load image files from a folder"""
    extensions = ['.jpg', '.jpeg', '.png', '.tiff', '.tif']
    
    all_files = []
    for ext in extensions:
        pattern = os.path.join(folder_path, f"*{ext}")
        all_files.extend(glob.glob(pattern))
        pattern = os.path.join(folder_path, f"*{ext.upper()}")
        all_files.extend(glob.glob(pattern))
    
    all_files.sort()
    
    if not all_files:
        logger.warning(f"No image files found in {folder_path}")
    else:
        logger.info(f"Found {len(all_files)} image files in {folder_path}")
    
    return all_files


def read_image_file(file_path: str) -> np.ndarray:
    """Read an image file and convert it to a numpy array"""
    try:
        with Image.open(file_path) as img:
            if img.mode != 'L':
                img = img.convert('L')
            array = np.array(img, dtype=np.uint32)
            return array
    except Exception as e:
        logger.error(f"Error reading image file {file_path}: {e}")
        return np.zeros((10, 10), dtype=np.uint32)


def save_url_to_file(url: str, file_path: str, metadata: dict = None):
    """Save Tiled URL and metadata to a file"""
    data = {
        "tiled_url": url,
        "timestamp": datetime.now().isoformat(),
        "metadata": metadata or {}
    }
    
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
            logger.info(f"Saved Tiled URL to {file_path}")
    except Exception as e:
        logger.error(f"Error saving URL to file: {e}")


async def ingest_to_tiled(client, container_name: str, image_files: List[str]) -> tuple:
    """Ingest images to Tiled one by one and return the URL and metadata"""
    try:
        # Create container
        try:
            container = client.create_container(key=container_name)
            logger.info(f"Created container: {container_name}")
        except Exception as e:
            logger.info(f"Container may already exist, trying to access: {e}")
            container = client[container_name]
        
        # Create a timestamp-based unique identifier for this run
        timestamp = int(time.time())
        run_id = f"local_images_{timestamp}"
        
        # Process the first image to get dimensions and create metadata
        first_image = read_image_file(image_files[0])
        height, width = first_image.shape
        data_type = str(first_image.dtype)
        
        # Initialize metadata
        metadata = {
            "source": "local_image_sim",
            "timestamp": datetime.now().isoformat(),
            "num_images": len(image_files),
            "width": width,
            "height": height,
            "data_type": data_type,
        }
        
        # Process each image
        for i, file_path in enumerate(image_files):
            if i % 10 == 0:
                logger.info(f"Processing image {i+1}/{len(image_files)}")
            
            # Read image
            image = read_image_file(file_path)
            
            # Create image key
            image_key = f"{run_id}_{i:04d}"
            
            # Create image metadata
            image_metadata = {
                "index": i,
                "filename": os.path.basename(file_path),
                "timestamp": datetime.now().isoformat(),
            }
            
            # Write image to Tiled
            container.write_array(
                key=image_key,
                array=image,
                metadata=image_metadata
            )
            
            # Clear memory
            del image
        
        # Write overall metadata
        container.write_array(
            key=f"{run_id}_metadata",
            array=np.array([0]),  # Minimal array
            metadata=metadata
        )
        
        # Construct and return the Tiled URL for the ingested data
        base_url = client.uri.replace("/metadata", "")
        tiled_url = f"{base_url}/array/full/{container_name}/{run_id}"
        logger.info(f"Data ingested to Tiled at URL: {tiled_url}")
        
        # Store information for simulator to find all images
        metadata["image_pattern"] = f"{run_id}_[0-9]{{4}}"
        
        return tiled_url, metadata
        
    except Exception as e:
        logger.error(f"Error ingesting data to Tiled: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


@app.command()
def main(
    image_folder: str = typer.Option(DEFAULT_IMAGE_FOLDER, help="Path to folder containing image files"),
    tiled_uri: str = typer.Option(TILED_URI, help="URI of the Tiled server"),
    api_key: str = typer.Option(TILED_API_KEY, help="API key for Tiled authentication"),
    container: str = typer.Option(TILED_CONTAINER, help="Name of the Tiled container"),
    url_file: str = typer.Option(URL_FILE, help="Path to file to save the Tiled URL"),
):
    """
    Read images from a local folder, ingest them to a Tiled server one by one,
    and save the resulting URL to a local file for later use.
    """
    logger.info(f"Starting Local Image Ingestion with:")
    logger.info(f"- Image folder: {image_folder}")
    logger.info(f"- Tiled URI: {tiled_uri}")
    logger.info(f"- URL file: {url_file}")
    
    async def run():
        # Check if image folder exists
        if not os.path.exists(image_folder):
            logger.error(f"Image folder not found: {image_folder}")
            return
        
        # Load image files
        image_files = load_image_files(image_folder)
        if not image_files:
            logger.error(f"No image files found in {image_folder}")
            return
        
        # Connect to Tiled server and ingest images
        try:
            client = from_uri(tiled_uri, api_key=api_key)
            logger.info(f"Connected to Tiled server at {tiled_uri}")
            
            tiled_url, metadata = await ingest_to_tiled(client, container, image_files)
            
            # Save the URL to file
            save_url_to_file(tiled_url, url_file, metadata)
            logger.info(f"Successfully ingested {len(image_files)} images to Tiled")
            logger.info(f"Tiled URL: {tiled_url}")
            
        except Exception as e:
            logger.error(f"Failed to ingest images to Tiled: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    asyncio.run(run())


if __name__ == "__main__":
    app()