import asyncio
import logging

import typer

from arroyosas.zmq import ZMQFrameListener

from ..config import settings
from ..log_utils import setup_logger
from ..one_d_reduction.operator import OneDReductionOperator
from ..tiled.publisher import Tiled1DResultsPublisher
from ..websockets import OneDWSPublisher

app = typer.Typer()
logger = logging.getLogger("arroyosas")
app_settings = settings.viz_operator
setup_logger(logger, log_level=settings.logging_level)


@app.command()
async def start():
    logger.info("Starting Tiled Poller")
    logger.info("Getting settings")
    logger.info(f"{settings.viz_operator}")
    operator = OneDReductionOperator.from_settings(app_settings.operator)
    tiled_event_publisher = Tiled1DResultsPublisher.from_settings(
        app_settings.publishers.tiled
    )
    ws_publisher = OneDWSPublisher.from_settings(app_settings.ws_publisher)
    operator.add_publisher(ws_publisher)
    operator.add_publisher(tiled_event_publisher)
    listener = ZMQFrameListener.from_settings(app_settings.publishers.websockets, operator)
    await asyncio.gather(listener.start(), ws_publisher.start())


if __name__ == "__main__":
    asyncio.run(start())
