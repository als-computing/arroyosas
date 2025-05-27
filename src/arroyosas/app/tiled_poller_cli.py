import asyncio
import logging

import typer

from arroyosas.config import settings
from arroyosas.tiled.tiled import TiledPollingRedisListener, TiledRawFrameOperator
from arroyosas.zmq import ZMQFramePublisher

from ..log_utils import setup_logger

app = typer.Typer()
logger = logging.getLogger("arroyosas")

app_settings = settings.tiled_poller
setup_logger(logger, log_level=settings.logging_level)
             

@app.command()
async def start(tiled_url: str, zmq_url: str, poll_interval: int = 5):
    logger.info(
        f"Starting Tiled Poller with tiled_url: {tiled_url}, zmq_url: {zmq_url}, poll_interval: {poll_interval}"
    )
    operator = TiledRawFrameOperator()
    publisher = ZMQFramePublisher.from_settings(app_settings.zmq_frame_publisher)

    operator.add_publisher(publisher)
    listener = TiledPollingRedisListener.from_settings(app_settings, operator)
    await asyncio.gather(listener.start())


if __name__ == "__main__":
    asyncio.run(start("", ""))
