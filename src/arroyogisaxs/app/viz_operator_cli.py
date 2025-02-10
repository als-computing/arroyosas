import asyncio
import logging

import typer
import zmq

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

    zmq_connect = f"tcp://{app_settings.listen_address}:{app_settings.listen_port}"
    logger.info(zmq_connect)
    ctx = zmq.asyncio.Context()
    listen_zmq_socket = ctx.socket(zmq.SUB)
    listen_zmq_socket.setsockopt(zmq.RCVHWM, 100000)
    listen_zmq_socket.setsockopt(zmq.SUBSCRIBE, b"")
    listen_zmq_socket.connect(zmq_connect)

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
