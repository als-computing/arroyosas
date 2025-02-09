import asyncio
import logging

import typer

from ..log_utils import setup_logger

# import signal


app = typer.Typer()
logger = logging.getLogger("tr_ap_xps")
setup_logger(logger)


@app.command()
async def start() -> None:
    pass
    # try:
    #     logger.setLevel(app_settings.log_level.upper())
    #     logger.debug("DEBUG LOGGING SET")

    #     received_sigterm = {"received": False}  # Define the variable received_sigterm

    #     # setup websocket server
    #     operator = XPSOperator()
    #     ws_publisher = XPSWSResultPublisher(
    #         host=app_settings.websockets_publisher.host,
    #         port=app_settings.websockets_publisher.port,
    #     )

    #     operator.add_publisher(ws_publisher)
    #     operator.add_publisher(tiled_pub)
    #     # connect to labview zmq

    #     lv_zmq_socket = setup_zmq()
    #     listener = XPSLabviewZMQListener(operator=operator, zmq_socket=lv_zmq_socket)

    #     # Wait for both tasks to complete
    #     await asyncio.gather(listener.start(), ws_publisher.start())

    #     def handle_sigterm(signum, frame):
    #         logger.info("SIGTERM received, stopping...")
    #         received_sigterm["received"] = True
    #         asyncio.create_task(listener.stop())
    #         asyncio.create_task(ws_publisher.stop())

    #     # Register the handler for SIGTERM
    #     signal.signal(signal.SIGTERM, handle_sigterm)
    # except Exception as e:
    #     logger.error(f"Error setting up XPS processor {e}")
    #     raise e


if __name__ == "__main__":
    asyncio.run(start())
