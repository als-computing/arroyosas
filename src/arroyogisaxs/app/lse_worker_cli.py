import asyncio
import logging

import msgpack
import typer
import zmq

from ..config import settings
from ..log_utils import setup_logger
from ..lse.lse_reducer import LatentSpaceReducer

app = typer.Typer()
logger = logging.getLogger("arroyogisaxs")
setup_logger(logger)


@app.command()
async def start() -> None:
    app_settings = settings.lse
    logger.info("Getting settings")
    logger.info(f"{settings.lse}")

    context = zmq.Context()
    client_socket = context.socket(zmq.REP)  # worker to the broker
    client_socket.connect(app_settings.zmq_router_address)
    logger.info(f"Connected to broker at {app_settings.zmq_router_address}")
    reducer = LatentSpaceReducer().with_models_loaded()

    while True:
        try:
            raw_msg = await client_socket.recv()
            message = msgpack.unpackb(raw_msg, raw=False)
            message_type = message.get("msg_type")
            if message_type != "event":
                continue
            latent_space = await reducer.reduce(message)
            await client_socket.send(msgpack.packb(latent_space, use_bin_type=True))
        except Exception as e:
            logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    asyncio.run(start())
