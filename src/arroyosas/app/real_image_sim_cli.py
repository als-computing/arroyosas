import asyncio
import os
import logging
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
Simulates image retrieval from Tiled and sends them onto ZMQ,
taking care of pydantic messages, serialization and msgpack
"""

# Default Tiled configuration - use the exact URI format from data_simulator.py
DATA_TILED_URI = (
    "https://tiled-demo.blueskyproject.io/api/v1/metadata/rsoxs/raw/"
    "468810ed-2ff9-4e92-8ca9-dcb376d01a56/primary/data/Small Angle CCD Detector_image"
)
TILED_API_KEY = os.getenv("DATA_TILED_KEY")
if TILED_API_KEY == "":
    TILED_API_KEY = None

# Frame configuration
FRAME_WIDTH = 1024
FRAME_HEIGHT = 1026
DATA_TYPE = "uint32"

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()


def get_num_frames(tiled_uri, tiled_api_key=None):
    """Get the number of available frames from Tiled, following data_simulator.py pattern"""
    client = from_uri(tiled_uri, api_key=tiled_api_key)
    return client.shape[0] if hasattr(client, 'shape') and len(client.shape) > 0 else 0


async def process_images_from_tiled(
    socket: zmq.asyncio.Socket, 
    cycles: int, 
    frames: int, 
    pause: float,
    tiled_uri: str,
    tiled_api_key: str = None
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


@app.command()
def main(
    cycles: int = 10000, 
    frames: int = 50, 
    pause: float = 5,
    tiled_uri: str = None,
    api_key: str = None
):
    """
    Run the image simulator that reads frames from Tiled and publishes them via ZMQ.
    
    Args:
        cycles: Number of cycles to run
        frames: Maximum number of frames per cycle
        pause: Pause time between cycles in seconds
        tiled_uri: URI of the Tiled server (defaults to the predefined DATA_TILED_URI)
        api_key: API key for Tiled authentication (defaults to env var DATA_TILED_KEY)
    """
    # Use provided values or fall back to defaults
    tiled_uri = tiled_uri or DATA_TILED_URI
    api_key = api_key or TILED_API_KEY
    
    async def run():
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.PUB)
        address = settings.tiled_poller.publish_address
        logger.info(f"Binding to ZMQ address: {address}")
        socket.bind(address)
        await process_images_from_tiled(socket, cycles, frames, pause, tiled_uri, api_key)
        return

    asyncio.run(run())


if __name__ == "__main__":
    app()