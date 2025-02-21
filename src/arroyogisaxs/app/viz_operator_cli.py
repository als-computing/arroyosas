import asyncio
import logging

import typer

from arroyogisaxs.zmq import ZMQFrameListener

from ..config import settings
from ..log_utils import setup_logger
from ..one_d_reduction.operator import OneDReductionOperator

from ..tiled import TiledProcessedPublisher
from ..websockets import OneDWSPublisher

app = typer.Typer()
logger = logging.getLogger("arroyogisaxs")
app_settings = settings.viz_operator
setup_logger(logger, log_level=settings.logging_level)


@app.command()
async def start():
    
    logger.info("Starting Tiled Poller")
    logger.info("Getting settings")
    logger.info(f"{settings.viz_operator}")
    operator = OneDReductionOperator.from_settings(app_settings)
    tiled_event_publisher = TiledProcessedPublisher.from_settings(
        settings.tiled_processed
    )
    ws_publisher = OneDWSPublisher.from_settings(app_settings.ws_publisher)
    operator.add_publisher(ws_publisher)
    operator.add_publisher(tiled_event_publisher)
    listener = ZMQFrameListener.from_settings(app_settings.listener, operator)
    await asyncio.gather(listener.start(), ws_publisher.start())


if __name__ == "__main__":
    asyncio.run(start())
