import asyncio
import time

# from arroyopy.schemas import NumpyArrayModel
import msgpack
import numpy as np
import typer
import zmq
import zmq.asyncio

from ..config import settings
from ..schemas import (
    GISAXSRawEvent,
    GISAXSRawStart,
    GISAXSRawStop,
    SerializableNumpyArrayModel,
)

"""
Simulates however we are going to get images and sends them
onto ZMQ, taking care of pydantic messages, serialization and msgpack
"""


FRAME_WIDTH = 100
FRAME_HEIGHT = 100
DATA_TYPE = "float32"

app = typer.Typer()


async def process_images(
    socket: zmq.asyncio.Socket, cycles: int, frames: int, pause: float
):
    for cycle_num in range(cycles):
        start = GISAXSRawStart(
            width=FRAME_WIDTH,
            height=FRAME_HEIGHT,
            data_type=DATA_TYPE,
            tiled_url="tbd://run_url",
        )
        print("start")
        await socket.send(msgpack.packb(start.model_dump()))

        for frame_num in range(frames):
            # Create a test pattern image that changes slightly each time
            frame_number = int(time.time()) % 100  # Change pattern every second
            image = np.zeros((FRAME_WIDTH, FRAME_HEIGHT), dtype=DATA_TYPE)
            np.fill_diagonal(image, frame_number % 255)

            event = GISAXSRawEvent(
                image=SerializableNumpyArrayModel(array=image),
                frame_number=frame_num,
                tiled_url="tb://frame_url",
            )
            print("event")
            await socket.send(msgpack.packb(event.model_dump()))
        stop = GISAXSRawStop(num_frames=frames)
        print("stop")
        await socket.send(msgpack.packb(stop.model_dump()))
        await asyncio.sleep(pause)
        print(f"Cycle {cycle_num} complete sent {frames} frames")

    print("All cycles complete")
    return


@app.command()
def main(cycles: int = 10000, frames: int = 5, pause: float = 5):
    async def run():
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.PUB)
        address = f"tcp://{settings.tiled_poller.publish_address}:{settings.tiled_poller.publish_port}"
        print(f"Connecting to {address}")
        socket.bind(address)
        await process_images(socket, cycles, frames, pause)
        return

    asyncio.run(run())


if __name__ == "__main__":
    app()
