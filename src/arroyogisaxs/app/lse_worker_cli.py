import logging

import msgpack
import typer
import zmq
import zmq.asyncio

from ..config import settings
from ..log_utils import setup_logger
from ..lse.lse_reducer import LatentSpaceReducer
from ..schemas import (
    GISAXSLatentSpaceEvent,
    GISAXSRawEvent,
    SerializableNumpyArrayModel,
)

app = typer.Typer()
logger = logging.getLogger("arroyogisaxs")
setup_logger(logger)


@app.command()
def start() -> None:
    app_settings = settings.lse_worker
    logger.info("Getting settings")
    logger.info(f"{settings}")

    context = zmq.Context()
    client_socket = context.socket(zmq.REP)  # worker to the broker
    client_socket.connect(app_settings.broker.dealer_address)
    logger.info(
        f"Connected to broker dealer at {app_settings.broker.dealer_address}"
    )
    reducer = LatentSpaceReducer.from_settings(settings.lse_reducer)
    logger.info("Listening for messages")
    while True:
        response_sent = False
        try:
            raw_msg = client_socket.recv()
            message = msgpack.unpackb(raw_msg, raw=False)
            message_type = message.get("msg_type")
            if message_type != "event":
                continue
            image = SerializableNumpyArrayModel.deserialize_array(message["image"])
            message["image"] = image
            event = GISAXSRawEvent(**message)
            # logger.debug("calculating latent space")

            latent_space = reducer.reduce(event)
            # logger.debug("latent space returned")
            return_message = GISAXSLatentSpaceEvent(
                tiled_url="foo",
                feature_vector=latent_space[0].tolist(),
                index=message.get("frame_number"),
            )
            client_socket.send(
                msgpack.packb(return_message.model_dump(), use_bin_type=True)
            )
            response_sent = True
            # logger.debug("LSE returned")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            if not response_sent:
                client_socket.send(b"ERROR")


if __name__ == "__main__":
    start()
