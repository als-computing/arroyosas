import asyncio
import time
from datetime import datetime

# from arroyopy.schemas import NumpyArrayModel
import msgpack
import numpy as np
import typer
import zmq
import zmq.asyncio

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

FRAME_WIDTH = 475
FRAME_HEIGHT = 619
DATA_TYPE = "uint8"

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
            tiled_url="tbd://run_url",
            run_name="test_run",
            run_id=str(current_time),
        )
        print("start")
        await socket.send(msgpack.packb(start.model_dump()))

        for frame_num in range(frames):
            # Create a test pattern image that changes slightly each time
            frame_number = int(time.time()) % 100  # Change pattern every second
            image = np.random.rand(FRAME_WIDTH, FRAME_HEIGHT).astype(DATA_TYPE)
            np.fill_diagonal(image, frame_number % 255)

            event = RawFrameEvent(
                image=SerializableNumpyArrayModel(array=image),
                frame_number=frame_num,
                tiled_url="tb://frame_url",
            )
            print("event")
            await socket.send(msgpack.packb(event.model_dump()))
            await asyncio.sleep(0.5)
        stop = SASStop(num_frames=frames)
        print("stop")
        await socket.send(msgpack.packb(stop.model_dump()))
        await asyncio.sleep(pause)
        print(f"Cycle {cycle_num} complete sent {frames} frames")

    print("All cycles complete")
    return


@app.command()
def main(cycles: int = 10000, frames: int = 50, pause: float = 5):
    async def run():
        print("Starting frame listener simulation")
        print(f"Cycles: {cycles}, Frames: {frames}, Pause: {pause}")
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.PUB)
        address = settings.frame_listener_sim.zmq_frame_publisher.address
        print(f"Connecting to {address}")
        socket.bind(address)
        await process_images(socket, cycles, frames, pause)
        return

    asyncio.run(run())


if __name__ == "__main__":
    app()
