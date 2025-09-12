import asyncio
import logging
import os
import aiosqlite
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
# New environment variable for selecting prod vs dev environment
TILED_ENV = os.getenv("TILED_ENV", "dev").lower()

# Define environment-specific URLs
TILED_URLS = {
    "dev": {
        "url_pattern": "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/"
    },
    "prod": {
        "url_pattern": "http://tiled.nsls2.bnl.gov/api/v1/array/full/"
    }
}

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()


async def get_urls_from_db(db_path, limit=None):
    """Get a list of Tiled URLs from the database asynchronously"""
    try:
        async with aiosqlite.connect(db_path) as conn:
            query = "SELECT id, tiled_url FROM vectors ORDER BY id"
            if limit:
                query += f" LIMIT {limit}"
            
            async with conn.execute(query) as cursor:
                results = await cursor.fetchall()
            
            if not results:
                logger.warning(f"No Tiled URLs found in database {db_path}")
                return []
                
            logger.info(f"Found {len(results)} Tiled URLs in database")
            return results
    except Exception as e:
        logger.error(f"Error reading from database: {e}")
        return []


def transform_url_for_env(tiled_url, env):
    """
    Transform a Tiled URL to match the specified environment format.
    
    Args:
        tiled_url: The original Tiled URL (typically from dev environment)
        env: The target environment ('dev' or 'prod')
        
    Returns:
        str: Transformed URL for the target environment
    """
    if env not in TILED_URLS:
        logger.warning(f"Unknown environment '{env}', falling back to 'dev'")
        env = "dev"
    
    # If we're staying in dev, no transformation needed
    if env == "dev" and "tiled-dev.nsls2.bnl.gov" in tiled_url:
        return tiled_url
    
    # Extract slice parameter if present
    slice_param = None
    if '?' in tiled_url:
        slice_param = tiled_url.split('?')[1]
    
    # Parse the URL to extract UUID and stream path
    url_without_query = tiled_url.split('?')[0]  # Remove query parameters
    
    # Extract UUID and stream path
    uuid = None
    stream_path = None
    
    if 'array/full/' in url_without_query:
        path_after_full = url_without_query.split('array/full/')[1]
        parts = path_after_full.split('/')
        if len(parts) >= 1:
            uuid = parts[0]
            if len(parts) > 1:
                stream_path = '/'.join(parts[1:])
    
    if not uuid or not stream_path:
        logger.error(f"Could not parse Tiled URL: {tiled_url}")
        return tiled_url  # Return original if parsing fails
    
    # Transform URL based on environment
    if env == "prod":
        # Get image name from stream_path
        parts = stream_path.split('/')
        if len(parts) > 0:
            image_name = parts[-1]
            # Format: http://tiled.nsls2.bnl.gov/api/v1/array/full/smi/raw/{uuid}/primary/data/{image_name}?slice=...
            new_url = f"{TILED_URLS[env]['url_pattern']}smi/raw/{uuid}/primary/data/{image_name}"
        else:
            # Fallback if we can't extract image name
            logger.error(f"Could not extract image name from stream path: {stream_path}")
            return tiled_url
    else:
        # Dev URL format: http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/{uuid}/{stream_path}?slice=...
        new_url = f"{TILED_URLS[env]['url_pattern']}{uuid}/{stream_path}"
    
    # Add slice parameter if it exists
    if slice_param:
        new_url = f"{new_url}?{slice_param}"
    
    logger.debug(f"Transformed URL: {tiled_url} -> {new_url}")
    return new_url


def _read_image_from_tiled_url_sync(tiled_url, api_key=None):
    """
    Read an image from a Tiled URL.
    
    Args:
        tiled_url: The Tiled URL (already transformed for the appropriate environment)
        api_key: API key for Tiled authentication
        
    Returns:
        tuple: (image_data, index)
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


async def read_image_from_tiled_url(tiled_url, api_key=None):
    """Async wrapper for _read_image_from_tiled_url_sync"""
    return await asyncio.to_thread(_read_image_from_tiled_url_sync, tiled_url, api_key)


@app.command()
def main(
    db_path: str = typer.Option(DEFAULT_DB_PATH, help="Path to the SQLite database containing Tiled URLs"),
    max_frames: int = typer.Option(10000, help="Maximum number of frames to process"),
    api_key: str = typer.Option(DEFAULT_API_KEY, help="API key for Tiled authentication"),
    env: str = typer.Option(TILED_ENV, help="Tiled environment to use ('dev' or 'prod')")
):
    """
    Run the image simulator that reads Tiled URLs from a database, fetches the images, and publishes them via ZMQ.
    
    Configuration can be set via environment variables:
    - DB_PATH: Path to the SQLite database
    - TILED_API_KEY: API key for Tiled authentication
    - TILED_ENV: Environment to use ('dev' or 'prod')
    
    Command-line arguments override environment variables.
    """
    # Log the configuration
    logger.info(f"Starting DB Image Simulator with:")
    logger.info(f"- Database path: {db_path}")
    logger.info(f"- Max frames: {max_frames}")
    logger.info(f"- API key provided: {api_key is not None}")
    logger.info(f"- Tiled environment: {env}")
    
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
        urls = await get_urls_from_db(db_path, limit=max_frames)
        if not urls:
            logger.error("No URLs found in database, cannot continue")
            return
            
        # Send start event
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start = SASStart(
            width=1679,    # Default values - these will be updated from real data
            height=1475,
            data_type="uint32",
            tiled_url=f"{env}://latent_vectors",
            run_name=f"{env}_tiled_run",
            run_id=str(current_time),
        )
        logger.info(f"Sending start event")
        await socket.send(msgpack.packb(start.model_dump()))
        
        # Process each URL
        for db_id, tiled_url in urls:
            try:
                logger.info(f"Processing URL from DB record {db_id}: {tiled_url}")
                
                # Transform the URL for the current environment before processing
                transformed_url = transform_url_for_env(tiled_url, env)
                logger.info(f"Transformed URL: {transformed_url}")
                
                # Read image data from transformed Tiled URL
                image_data, index = await read_image_from_tiled_url(transformed_url, api_key)
                
                if image_data is None:
                    logger.error(f"Failed to read image from {transformed_url}")
                    continue
                
                # Send the frame event with transformed URL
                event = RawFrameEvent(
                    image=SerializableNumpyArrayModel(array=image_data),
                    frame_number=index,
                    tiled_url=transformed_url,
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