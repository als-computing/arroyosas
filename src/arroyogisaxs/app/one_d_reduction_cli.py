import asyncio
import logging

import redis
import typer

from ..config import settings
from ..log_utils import setup_logger
from ..shared_settings import SharedSettings
from ..tiled import Tiled1DPublisher
from ..viz_reduce import GISAXSVizReduceOperator
from ..websockets import GISAXSWS1DPublisher
from ..zmq import ZMQFrameListener

# import signal


app = typer.Typer()
logger = logging.getLogger(__name__)
setup_logger(logger)


app_settings = settings


@app.command()
async def start() -> None:
    pass
    try:
        logger.setLevel(app_settings.log_level.upper())
        logger.debug("DEBUG LOGGING SET")

        received_sigterm = {"received": False}  # Define the variable received_sigterm

        redis_client = redis.Redis(
            host=app_settings.redis.host, port=app_settings.redis.port, db=0
        )
        shared_settings = SharedSettings(redis_client)  # redis or kvrocks
        operator = GISAXSVizReduceOperator(shared_settings)
        tiled_1d_pub = GISAXSWS1DPublisher()
        ws_1D_publisher = Tiled1DPublisher()

        operator.add_publisher(tiled_1d_pub)
        operator.add_publisher(ws_1D_publisher)

        listener = ZMQFrameListener()
        # Wait for both tasks to complete
        await asyncio.gather(
            listener.start(), tiled_1d_pub.start(), ws_1D_publisher.start()
        )

        def handle_sigterm(signum, frame):
            logger.info("SIGTERM received, stopping...")
            received_sigterm["received"] = True
            asyncio.create_task(listener.stop())
            # asyncio.create_task(ws_publisher.stop())

        # Register the handler for SIGTERM
        # signal.signal(signal.SIGTERM, handle_sigterm)
    except Exception as e:
        logger.error(f"Error setting up XPS processor {e}")
        raise e


if __name__ == "__main__":
    asyncio.run(start())
