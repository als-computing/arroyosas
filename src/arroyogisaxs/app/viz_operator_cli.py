import asyncio
import logging

import typer
import zmq
import zmq.asyncio

from arroyogisaxs.zmq import ZMQListener

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
    logger.info(f"Listening to ZMQ on: {app_settings.viz_operator.zmq_listen_address}")
    logger.inro(f"HWM: {app_settings.viz_operator.zmq_hwm}")

    ctx = zmq.asyncio.Context()
    listen_zmq_socket = ctx.socket(zmq.SUB)
    listen_zmq_socket.setsockopt(zmq.RCVHWM, app_settings.viz_operator.zmq_hwm)
    listen_zmq_socket.setsockopt(zmq.SUBSCRIBE, b"")
    listen_zmq_socket.connect(app_settings.viz_operator.zmq_listen_address)

    operator = OneDReductionOperator()
    ws_publisher = OneDWSResultPublisher(
        host=app_settings.websocket_publish_host,
        port=app_settings.websocket_publish_port,
    )
    operator.add_publisher(ws_publisher)
    listener = ZMQListener(operator, listen_zmq_socket)
    await asyncio.gather(listener.start(), ws_publisher.start())


if __name__ == "__main__":
    asyncio.run(start())
