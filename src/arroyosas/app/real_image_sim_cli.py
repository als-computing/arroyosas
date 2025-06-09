import asyncio
import os
from datetime import datetime
from glob import glob

# from arroyopy.schemas import NumpyArrayModel
import msgpack
import numpy as np
import typer
import zmq
import zmq.asyncio
from PIL import Image

from ..config import settings
from ..schemas import (
    RawFrameEvent,
    SASStart,
    SASStop,
    SerializableNumpyArrayModel,
)

"""
Simulates however we are going to get images and sends them
onto ZMQ, taking care of pydantic messages, serialization and msgpack
"""

FRAME_WIDTH = 1475
FRAME_HEIGHT = 619
DATA_TYPE = "float32"

app = typer.Typer()


async def process_images(
    socket: zmq.asyncio.Socket, cycles: int, frames: int, pause: float
):
    for cycle_num in range(cycles):
        # Get current time formatted as YYYY-MM-DD HH:MM:SS
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start = SASStart(
            width=FRAME_WIDTH,
            height=FRAME_HEIGHT,
            data_type=DATA_TYPE,
            tiled_url="http://tiled:8000",
            run_name="test_run",
            run_id=str(current_time),
        )
        print("start")
        await socket.send(msgpack.packb(start.model_dump()))
        files = glob("/data/test_data/blade/*.tif")
        frame_num = 0
        for file in files:
            with Image.open(file) as image:
                image_array = np.array(image)
            event = RawFrameEvent(
                image=SerializableNumpyArrayModel(array=image_array),
                frame_number=frame_num,
                tiled_url="tb://frame_url",
            )
            print(
                f"Sending event with image shape={image_array.shape}, dtype={image_array.dtype}"
            )
            await socket.send(msgpack.packb(event.model_dump()))
            frame_num += 1
            # Limit frames per cycle if needed
            if frame_num >= frames:
                break
            # Add a small delay between frames to avoid overwhelming the receiver
            await asyncio.sleep(0.5)
        stop = SASStop(num_frames=frames)
        print("stop")
        await socket.send(msgpack.packb(stop.model_dump()))
        await asyncio.sleep(pause)
        print(f"Cycle {cycle_num} complete sent {frame_num} frames")

    print("All cycles complete")
    return


@app.command()
def main(cycles: int = 10000, frames: int = 50, pause: float = 5):
    async def run():
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.PUB)
        address = settings.tiled_poller.publish_address
        print(f"Connecting to {address}")
        socket.bind(address)
        await process_images(socket, cycles, frames, pause)
        return

    asyncio.run(run())


if __name__ == "__main__":
    app()
