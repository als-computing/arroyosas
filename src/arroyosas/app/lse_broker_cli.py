import asyncio
import logging

import typer

from ..config import settings
from ..log_utils import setup_logger
from ..zmq import ZMQBroker

app = typer.Typer()
logger = logging.getLogger("arroyosas")
setup_logger(logger)


@app.command()
async def start() -> None:
    app_settings = settings.lse_operator
    logger.info("Getting settings")
    logger.info(f"{settings.lse_operator}")
    logger.info("Starting Broker")
    # we may consider starting this in its own process
    broker = ZMQBroker.from_settings(app_settings.zmq_broker)
    await asyncio.gather(broker.start())


if __name__ == "__main__":
    asyncio.run(start())
