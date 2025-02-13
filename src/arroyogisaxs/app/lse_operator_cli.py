import asyncio
import logging

import typer
import zmq

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
    app_settings = settings.lse
    logger.info("Getting settings")
    logger.info(f"{settings.lse}")

    # start the ZMQBroker
    # we may consider starting this in its own process
    broker = ZMQBroker().from_settings(app_settings)
    broker.start()
    logger.info("Broker started")

    logger.info("Starting ZMQ PubSub Listener")
    logger.info(f"ZMQPubSubListener settings: {app_settings}")
    logger.info(f"Starting frame lister on {app_settings.zmq_listen_address}")

    ctx = zmq.asyncio.Context()
    listen_zmq_socket = ctx.socket(zmq.REQ)  # client to the broker
    listen_zmq_socket.setsockopt(zmq.RCVHWM, app_settings.zma_router_hwm)
    listen_zmq_socket.setsockopt(zmq.SUBSCRIBE, b"")
    listen_zmq_socket.connect(app_settings.zmq_listen_address)

    operator = LatentSpaceOperator()
    ws_publisher = OneDWSResultPublisher(
        host=app_settings.websocket_publish_host,
        port=app_settings.websocket_publish_port,
    )
    zmq_publisher = ZMQFramePublisher().from_settings(app_settings)
    operator.add_publisher(ws_publisher)
    operator.add_publisher(zmq_publisher)

    listener = ZMQPubSubListener(operator, listen_zmq_socket)
    await asyncio.gather(listener.start(), ws_publisher.start())


if __name__ == "__main__":
    asyncio.run(start())
