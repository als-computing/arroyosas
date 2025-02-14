import asyncio
import logging

import typer

from arroyogisaxs.tiled import TiledPollingFrameListener, TiledRawFrameOperator
from arroyogisaxs.zmq import ZMQFramePublisher

from ..log_utils import setup_logger

app = typer.Typer()
logger = logging.getLogger(__name__)
setup_logger(logger)


@app.command()
async def start(tiled_url: str, zmq_url: str, poll_interval: int = 5):
    logger.info(
        f"Starting Tiled Poller with tiled_url: {tiled_url}, zmq_url: {zmq_url}, poll_interval: {poll_interval}"
    )
    operator = TiledRawFrameOperator()
    publisher = ZMQFramePublisher()

    operator.add_publisher(publisher)
    listener = TiledPollingFrameListener(operator, "", "")
    await asyncio.gather(listener.start(), publisher.start())


if __name__ == "__main__":
    asyncio.run(start("", ""))
