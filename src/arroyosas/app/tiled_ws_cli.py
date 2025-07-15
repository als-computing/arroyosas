import asyncio
import logging

import typer

from arroyosas.config import settings
from arroyosas.log_utils import setup_logger
from arroyosas.tiled.tiled import TiledRawFrameOperator
from arroyosas.tiled.tiled_websocket import TiledWebSocketListener
from arroyosas.zmq import ZMQFramePublisher

app = typer.Typer()
logger = logging.getLogger("arroyosas")

app_settings = settings.tiled_poller
setup_logger(logger, log_level=settings.logging_level)

@app.command()
async def start(
    tiled_url: str = typer.Option(None, help="Tiled base URL"),
    websocket_url: str = typer.Option(None, help="WebSocket URL"),
    zmq_url: str = typer.Option(None, help="ZMQ publisher URL"),
    single_run: str = typer.Option(None, help="Process only a single run with the given ID"),
):
    """Start the Tiled WebSocket listener."""
    # Override settings if provided
    if tiled_url:
        app_settings.uri = tiled_url
    if websocket_url:
        app_settings.websocket_url = websocket_url
    if zmq_url:
        app_settings.zmq_frame_publisher.address = zmq_url
    if single_run:
        app_settings.single_run = single_run
    
    # Derive WebSocket URL if not provided
    if not app_settings.get("websocket_url"):
        base_url = app_settings.uri
        if base_url.endswith('/'):
            base_url = base_url[:-1]
        app_settings.websocket_url = base_url.replace('http://', 'ws://').replace('https://', 'wss://') + "/stream"
    
    # Create operator and publisher
    operator = TiledRawFrameOperator()
    publisher = ZMQFramePublisher.from_settings(app_settings.zmq_frame_publisher)
    operator.add_publisher(publisher)
    
    # Create and start listener
    listener = TiledWebSocketListener.from_settings(app_settings, operator)
    await listener.start()

if __name__ == "__main__":
    asyncio.run(start())