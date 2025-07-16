import asyncio
import logging

import typer

from ..config import settings
from ..log_utils import setup_logger
from ..lse.lse_operator import LatentSpaceOperator
from ..lse.lse_ws_publisher import LSEWSResultPublisher
from ..zmq import ZMQFrameListener

app = typer.Typer()
logger = logging.getLogger("arroyosas")
setup_logger(logger, settings.logging_level)


@app.command()
async def start() -> None:
    app_settings = settings.lse_operator
    logger.info("Getting settings")
    logger.info(f"{settings.lse_operator}")

    logger.info("Starting ZMQ PubSub Listener")
    logger.info(f"ZMQPubSubListener settings: {app_settings}")
    operator = LatentSpaceOperator.from_settings(app_settings, settings.lse_reducer)

    ws_publisher = LSEWSResultPublisher.from_settings(app_settings.ws_publisher)
    # tiled_event_publisher = TiledProcessedPublisher.from_settings(
    #     settings.tiled_processed
    # )
    operator.add_publisher(ws_publisher)
    # operator.add_publisher(tiled_event_publisher)

    listener = ZMQFrameListener.from_settings(app_settings.listener, operator)
    await asyncio.gather(listener.start(), ws_publisher.start())


if __name__ == "__main__":
    asyncio.run(start())
