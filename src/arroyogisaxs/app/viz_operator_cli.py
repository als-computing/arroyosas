import asyncio
import logging

import typer

from arroyogisaxs.zmq import ZMQFrameListener

from ..config import settings
from ..log_utils import setup_logger
from ..one_d_reduction.operator import OneDReductionOperator
from ..websockets import OneDWSResultPublisher

app = typer.Typer()
logger = logging.getLogger("arroyogisaxs")
setup_logger(logger)


@app.command()
async def start():
    app_settings = settings.viz_operator
    logger.info("Starting Tiled Poller")
    logger.info("Getting settings")
    logger.info(f"{settings.viz_operator}")
    operator = OneDReductionOperator.from_settings(
        app_settings, settings.smi_tiled_image_path
    )
    ws_publisher = OneDWSResultPublisher.from_settings(app_settings.ws_publisher)
    operator.add_publisher(ws_publisher)
    listener = ZMQFrameListener.from_settings(app_settings.listener, operator)
    await asyncio.gather(listener.start(), ws_publisher.start())


if __name__ == "__main__":
    asyncio.run(start())
