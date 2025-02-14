import asyncio
import logging

import typer

from ..config import settings
from ..log_utils import setup_logger
from ..lse.lse_operator import LatentSpaceOperator
from ..websockets import OneDWSResultPublisher
from ..zmq import ZMQBroker, ZMQFramePublisher, ZMQPubSubListener

app = typer.Typer()
logger = logging.getLogger("arroyogisaxs")
setup_logger(logger)


@app.command()
async def start() -> None:
    app_settings = settings.lse_operator
    logger.info("Getting settings")
    logger.info(f"{settings.lse_operator}")

    logger.info("Starting ZMQ PubSub Listener")
    logger.info(f"ZMQPubSubListener settings: {app_settings}")
    operator = LatentSpaceOperator()

    ws_publisher = OneDWSResultPublisher.from_settings(app_settings.ws_publisher)
    zmq_publisher = ZMQFramePublisher.from_settings(app_settings.zmq_publisher)
    operator.add_publisher(ws_publisher)
    operator.add_publisher(zmq_publisher)

    listener = ZMQPubSubListener.from_settings(app_settings.listener, operator)

    # we may consider starting this in its own process
    broker = ZMQBroker.from_settings(app_settings.router)

    await asyncio.gather(listener.start(), ws_publisher.start(), broker.start())


if __name__ == "__main__":
    asyncio.run(start())
