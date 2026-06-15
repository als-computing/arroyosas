import asyncio
import json
import logging
import os
import re
from datetime import datetime
from typing import List

import aiosqlite
import msgpack
import numpy as np
import typer
import zmq
import zmq.asyncio
from tiled.client import from_uri

from ..config import settings
from ..schemas import RawFrameEvent, SASStart, SASStop, SerializableNumpyArrayModel

"""
Unified simulator combining:
- db_replay_sim_cli.py
- local_tiled_sim_cli.py
- real_image_sim_cli.py
"""

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()

# =============================================================================
# FROM: db_replay_sim_cli.py
# =============================================================================
# Get configuration from environment variables with defaults
DEFAULT_DB_PATH = os.getenv("DB_PATH", "latent_vectors.db")
TILED_ENV = os.getenv("TILED_ENV", "dev").lower()

# Define environment-specific URLs
TILED_URLS = {
    "dev": {"url_pattern": "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/"},
    "prod": {"url_pattern": "http://tiled.nsls2.bnl.gov/api/v1/array/full/"},
}


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
    if "?" in tiled_url:
        slice_param = tiled_url.split("?")[1]

    # Parse the URL to extract UUID and stream path
    url_without_query = tiled_url.split("?")[0]  # Remove query parameters

    # Extract UUID and stream path
    uuid = None
    stream_path = None

    if "array/full/" in url_without_query:
        path_after_full = url_without_query.split("array/full/")[1]
        parts = path_after_full.split("/")
        if len(parts) >= 1:
            uuid = parts[0]
            if len(parts) > 1:
                stream_path = "/".join(parts[1:])

    if not uuid or not stream_path:
        logger.error(f"Could not parse Tiled URL: {tiled_url}")
        return tiled_url  # Return original if parsing fails

    # Transform URL based on environment
    if env == "prod":
        # Get image name from stream_path
        parts = stream_path.split("/")
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
        if "?" in tiled_url and "slice=" in tiled_url:
            slice_param = tiled_url.split("slice=")[1].split("&")[0]
            if ":" in slice_param:
                parts = slice_param.split(",")[0].split(":")
                if parts[0].isdigit():
                    index = int(parts[0])

        # Parse the URL to extract base URL and path
        url_without_query = tiled_url.split("?")[0]  # Remove query parameters
        url_parts = url_without_query.split("/api/v1/")

        if len(url_parts) != 2:
            logger.error(f"Invalid Tiled URL format: {tiled_url}")
            return None, 0

        # Change array/full to metadata
        base_uri = f"{url_parts[0]}/api/v1/metadata"

        # Extract dataset URI - get everything after "array/full/"
        full_path = url_parts[1]

        if "array/full/" in url_without_query:
            # If the URL contains array/full, extract the part after it
            path_parts = full_path.split("array/full/")
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


# =============================================================================
# FROM: local_tiled_sim_cli.py
# =============================================================================

# Default settings
URL_FILE = os.getenv("URL_FILE", "./tiled_url.json")


def load_url_from_file(file_path: str) -> tuple:
    """Load Tiled URL and metadata from a file"""
    try:
        if not os.path.exists(file_path):
            logger.error(f"URL file not found: {file_path}")
            return None, None

        with open(file_path, "r") as f:
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
        for char in "[]{}.":
            regex = regex.replace(char, "\\" + char)
        # Replace the digit pattern with actual regex pattern
        regex = regex.replace("\\[0-9\\]\\{4\\}", r"\d{4}")
        regex_pattern = re.compile(regex)

        logger.info(f"Using regex pattern: {regex}")

        # Filter keys by regex
        matching_keys = sorted([k for k in all_keys if regex_pattern.match(k)])
        logger.info(f"Found {len(matching_keys)} matching image keys")

        return matching_keys
    except Exception as e:
        logger.error(f"Error getting matching keys: {e}")
        return []


# =============================================================================
# FROM: real_image_sim_cli.py
# =============================================================================

# Default Tiled configuration - use the exact URI format from data_simulator.py
DATA_TILED_URI = (
    "https://tiled-demo.blueskyproject.io/api/v1/metadata/rsoxs/raw/"
    "468810ed-2ff9-4e92-8ca9-dcb376d01a56/primary/data/Small Angle CCD Detector_image"
)

# Frame configuration
FRAME_WIDTH = 1024
FRAME_HEIGHT = 1026
DATA_TYPE = "uint32"


def get_num_frames(tiled_uri, tiled_api_key=None):
    """Get the number of available frames from Tiled, following data_simulator.py pattern"""
    client = from_uri(tiled_uri, api_key=tiled_api_key)
    return client.shape[0] if hasattr(client, "shape") and len(client.shape) > 0 else 0


async def process_images_from_tiled(
    socket: zmq.asyncio.Socket,
    cycles: int,
    frames: int,
    pause: float,
    tiled_uri: str,
    tiled_api_key: str = None,
):
    """
    Process images from Tiled and send them via ZMQ
    """
    try:
        # Connect to Tiled server using the exact URI format
        logger.info(f"Connecting to Tiled server at {tiled_uri}")
        client = from_uri(tiled_uri, api_key=tiled_api_key)

        # Get total number of available frames
        total_frames = get_num_frames(tiled_uri, tiled_api_key)
        logger.info(f"Total frames available in Tiled: {total_frames}")

        for cycle_num in range(cycles):
            # Get current time formatted as YYYY-MM-DD HH:MM:SS
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Send SAS Start event
            start = SASStart(
                width=FRAME_WIDTH,
                height=FRAME_HEIGHT,
                data_type=DATA_TYPE,
                tiled_url=tiled_uri,
                run_name=f"tiled_run_{cycle_num}",
                run_id=str(current_time),
            )
            logger.info(f"Sending start event for cycle {cycle_num}")
            await socket.send(msgpack.packb(start.model_dump()))

            # Determine number of frames to process in this cycle
            frame_count = min(frames, total_frames)
            if frame_count == 0:
                logger.warning("No frames available in Tiled")
                continue

            # Process frames
            for frame_num in range(frame_count):
                try:
                    # Get frame data directly from Tiled client
                    # This is the correct way to read data from a Tiled array client
                    # The read() method returns a numpy array with the image data
                    image_array = client[frame_num]

                    # Create and send the frame event with the Tiled URI
                    event = RawFrameEvent(
                        image=SerializableNumpyArrayModel(array=image_array),
                        frame_number=frame_num,
                        tiled_url=f"{tiled_uri}?slice={frame_num}",
                    )
                    logger.info(f"Sending frame {frame_num} for cycle {cycle_num}")
                    await socket.send(msgpack.packb(event.model_dump()))

                except Exception as e:
                    logger.error(f"Error processing frame {frame_num}: {e}")

            # Send stop event
            stop = SASStop(num_frames=frame_count)
            logger.info(f"Sending stop event for cycle {cycle_num}")
            await socket.send(msgpack.packb(stop.model_dump()))

            await asyncio.sleep(pause)
            logger.info(f"Cycle {cycle_num} complete - sent {frame_count} frames")

        logger.info("All cycles complete")

    except Exception as e:
        logger.error(f"Error in processing images: {e}")


# =============================================================================
# UNIFIED CLI
# =============================================================================


@app.command()
def main(
    mode: str = typer.Option("direct", help="Simulator mode: 'direct', 'db_replay', or 'local_tiled'"),
    # db_replay parameters
    db_path: str = typer.Option(DEFAULT_DB_PATH, help="[db_replay] Path to the SQLite database"),
    max_frames: int = typer.Option(10000, help="[db_replay] Maximum number of frames to process"),
    env: str = typer.Option(TILED_ENV, help="[db_replay] Tiled environment ('dev' or 'prod')"),
    db_replay_api_key: str = typer.Option(None, help="[db_replay] API key for Tiled authentication"),
    # local_tiled parameters
    url_file: str = typer.Option(URL_FILE, help="[local_tiled] Path to file containing the Tiled URL"),
    cycles: int = typer.Option(1, help="[local_tiled/direct] Number of cycles to run"),
    pause: float = typer.Option(0.1, help="[local_tiled/direct] Pause time between frames"),
    cycle_pause: float = typer.Option(5.0, help="[local_tiled/direct] Pause time between cycles"),
    local_tiled_api_key: str = typer.Option(None, help="[local_tiled] API key for Tiled authentication"),
    # direct parameters
    frames: int = typer.Option(50, help="[direct] Maximum number of frames per cycle"),
    tiled_uri: str = typer.Option(DATA_TILED_URI, help="[direct] URI of the Tiled server"),
):
    """
    Unified simulator supporting three modes:
    - direct: Direct connection to Tiled URI (from real_image_sim_cli.py)
    - db_replay: Replay from SQLite database (from db_replay_sim_cli.py)
    - local_tiled: Read from JSON file (from local_tiled_sim_cli.py)
    """
    logger.info(f"Starting Unified Simulator in '{mode}' mode")

    async def run():
        # Setup ZMQ socket
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.PUB)
        address = settings.tiled_poller.zmq_frame_publisher.address
        logger.info(f"Binding to ZMQ address: {address}")
        socket.bind(address)

        if mode == "db_replay":
            # FROM: db_replay_sim_cli.py main()
            # Use db_replay_api_key or fallback to env var
            api_key = db_replay_api_key or os.getenv("TILED_LIVE_API_KEY")

            logger.info("DB replay mode with:")
            logger.info(f"- Database path: {db_path}")
            logger.info(f"- Max frames: {max_frames}")
            logger.info(f"- Tiled environment: {env}")
            logger.info(f"- API key provided: {api_key is not None}")

            if not os.path.exists(db_path):
                logger.error(f"Database file not found: {db_path}")
                return

            urls = await get_urls_from_db(db_path, limit=max_frames)
            if not urls:
                logger.error("No URLs found in database, cannot continue")
                return

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            start = SASStart(
                width=1679,
                height=1475,
                data_type="uint32",
                tiled_url=f"{env}://latent_vectors",
                run_name=f"{env}_tiled_run",
                run_id=str(current_time),
            )
            logger.info("Sending start event")
            await socket.send(msgpack.packb(start.model_dump()))

            for db_id, tiled_url in urls:
                try:
                    logger.info(f"Processing URL from DB record {db_id}: {tiled_url}")

                    transformed_url = transform_url_for_env(tiled_url, env)
                    logger.info(f"Transformed URL: {transformed_url}")

                    image_data, index = await read_image_from_tiled_url(transformed_url, api_key)

                    if image_data is None:
                        logger.error(f"Failed to read image from {transformed_url}")
                        continue

                    event = RawFrameEvent(
                        image=SerializableNumpyArrayModel(array=image_data),
                        frame_number=index,
                        tiled_url=transformed_url,
                    )
                    logger.info(f"Sending frame {index}")
                    await socket.send(msgpack.packb(event.model_dump()))

                    await asyncio.sleep(0.1)

                except Exception as e:
                    logger.error(f"Error processing frame from {tiled_url}: {e}")

            stop = SASStop(num_frames=len(urls))
            logger.info("Sending stop event")
            await socket.send(msgpack.packb(stop.model_dump()))
            logger.info(f"Complete - sent {len(urls)} frames")

        elif mode == "local_tiled":
            # FROM: local_tiled_sim_cli.py main()
            # Use local_tiled_api_key or fallback to env var
            api_key = local_tiled_api_key or os.getenv("TILED_LIVE_API_KEY")

            logger.info("Local tiled mode with:")
            logger.info(f"- URL file: {url_file}")
            logger.info(f"- Cycles: {cycles}")
            logger.info(f"- API key provided: {api_key is not None}")

            tiled_url, metadata = load_url_from_file(url_file)
            if not tiled_url:
                return

            url_without_query = tiled_url.split("?")[0]
            url_parts = url_without_query.split("/api/v1/")

            if len(url_parts) != 2:
                logger.error(f"Invalid Tiled URL format: {tiled_url}")
                return

            base_uri = f"{url_parts[0]}/api/v1/metadata"
            full_path = url_parts[1]

            if "array/full/" in url_without_query:
                path_parts = full_path.split("array/full/")
                if len(path_parts) > 1:
                    dataset_uri = path_parts[1]
                else:
                    dataset_uri = full_path
            else:
                dataset_uri = full_path

            logger.info(f"Connecting to Tiled server at {base_uri}")
            logger.info(f"Dataset URI: {dataset_uri}")

            client = from_uri(base_uri, api_key=api_key)

            parts = dataset_uri.split("/")
            if len(parts) != 2:
                logger.error(f"Invalid dataset URI format: {dataset_uri}")
                return

            container_name = parts[0]
            run_id_base = parts[1]

            if metadata and "image_pattern" in metadata:
                pattern = metadata["image_pattern"]
                matching_keys = await get_matching_keys(client, container_name, pattern)

                if not matching_keys:
                    logger.error(f"No matching keys found for pattern {pattern}")
                    return

                num_images = len(matching_keys)
            else:
                num_images = metadata.get("num_images", 0)
                if num_images <= 0:
                    logger.error("No image count found in metadata")
                    return

                matching_keys = [f"{run_id_base}_{i:04d}" for i in range(num_images)]

            logger.info(f"Found {len(matching_keys)} image keys")

            width = metadata.get("width")
            height = metadata.get("height")
            data_type = metadata.get("data_type")

            if not all([width, height, data_type]):
                logger.error("Missing image dimensions in metadata")
                return

            for cycle_num in range(cycles):
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

                for frame_num, image_key in enumerate(matching_keys):
                    try:
                        image_array = await fetch_image_from_tiled(client, container_name, image_key)

                        frame_url = f"{url_parts[0]}/api/v1/array/full/{container_name}/{image_key}?slice=0:1,0:1679,0:1475"

                        event = RawFrameEvent(
                            image=SerializableNumpyArrayModel(array=image_array),
                            frame_number=0,
                            tiled_url=frame_url,
                        )
                        logger.info(f"Sending image {image_key} for cycle {cycle_num}")
                        await socket.send(msgpack.packb(event.model_dump()))

                    except Exception as e:
                        logger.error(f"Error sending image {image_key}: {e}")
                        import traceback

                        logger.error(traceback.format_exc())
                        continue

                    await asyncio.sleep(pause)

                stop = SASStop(num_frames=len(matching_keys))
                logger.info(f"Sending stop event for cycle {cycle_num}")
                await socket.send(msgpack.packb(stop.model_dump()))

                if cycle_num < cycles - 1:
                    logger.info(f"Cycle {cycle_num} complete - pausing for {cycle_pause}s")
                    await asyncio.sleep(cycle_pause)

            logger.info(f"All {cycles} cycles complete")

        elif mode == "direct":
            # FROM: real_image_sim_cli.py main()
            logger.info("Direct mode with:")
            logger.info(f"- Tiled URI: {tiled_uri}")
            logger.info(f"- Cycles: {cycles}")
            logger.info(f"- Frames: {frames}")
            logger.info("- API key is not needed for public tiled dataset.")

            await process_images_from_tiled(socket, cycles, frames, pause, tiled_uri, tiled_api_key=None)

        else:
            logger.error(f"Unknown mode: {mode}. Use 'direct', 'db_replay', or 'local_tiled'")

    asyncio.run(run())


if __name__ == "__main__":
    app()
