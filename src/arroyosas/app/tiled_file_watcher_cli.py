import asyncio
import logging
import os
from pathlib import Path
from typing import Optional


from arroyopy import Listener, Operator, Publisher
from arroyopy.files import FileWatcherMessage
from tiled.client import from_uri
from tiled.client.base import BaseClient
import redis.asyncio as redis
import typer
from watchfiles import awatch, Change

from ..tiled.ingestor import TiledIngestor

# -----------------------------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------------------------
logger = logging.getLogger("data_watcher")


def setup_logging(log_level: str = "INFO"):
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False  # Prevent duplication through root logger






# -----------------------------------------------------------------------------
# CLI App
# -----------------------------------------------------------------------------
app = typer.Typer(help="Watch a directory and publish new .gb files to Redis.")


class RedisPublisher(Publisher):
    def __init__(self, redis_client: redis.Redis, channel_name: str):
        self.channel_name = channel_name
        self.redis_client = redis_client

    async def publish(self, message: FileWatcherMessage):
        logger.debug(f"Publishing message to Redis: {message}")
        if message.is_directory:
            logger.debug(f"Skipping directory: {message.file_path}")
            return
        await self.redis_client.publish(self.channel_name,message.model_dump_json())


class TiledPublisher(Publisher):
    def __init__(self, tiled_url: str, tiled_ingestor: TiledIngestor, redis_publisher: RedisPublisher = None):
        self.tiled_url = tiled_url
        self.tiled_ingestor = tiled_ingestor
        self.redis_publisher = redis_publisher
        super().__init__()

    async def publish(self, message: FileWatcherMessage):
        try:
            logger.debug(f"Publishing message to Tiled: {message}")
            if message.is_directory:  #  collections for directories get for new files by ingestor
                logger.debug(f"Skipping directory: {message.file_path}")
                return
            tiled_uri = await asyncio.to_thread(self.tiled_ingestor.add_scan_tiled, message.file_path)
            if self.redis_publisher:
                message.file_path = tiled_uri
                await self.redis_publisher.publish(message)
        except Exception as e:
            logger.error(f"Error publishing message: {e}")



class FileWatcherOperator(Operator):
    def __init__(self, publisher: Publisher):
        self.publisher = publisher

    async def process(self, message):
        logger.info(f"Processing message: {message}")
        await self.publisher.publish(message)


class NullPublisher(Publisher):
    async def publish(self, message):
        logger.debug(f"NullPublisher: {message} - No action taken.")

class FileWatcherListener(Listener):
    def __init__(self, directory: str, operator: Operator, force_polling: bool =  True):
        self.directory = directory
        self.operator = operator
        self.force_polling = force_polling

    async def start(self):
        logger.info(f"🔍 Watching directory recursively: {self.directory} (force_polling={self.force_polling})")
        async for changes in awatch(self.directory, force_polling=self.force_polling):
            for change_type, path_str in changes:
                if change_type is not Change.added:
                     continue
                path = Path(path_str)
                if not path.exists():
                    logger.debug(f"⚠️ Skipping non-existent path: {path}")
                    continue
    
                if not path.is_dir() and path.suffix not in [".gb", ".edf"]:
                    logger.debug(f"⚠️ Skipping non-supported file type: {path.suffix}")
                    continue
                
                logger.info(f"📦 Detected: {change_type} on {path}")
                message = FileWatcherMessage(
                    file_path=str(path), is_directory=path.is_dir()
                )
                await self.operator.process(message)
                

    async def stop(self):
        pass


@app.command()
def main(
    directory: Path = typer.Argument(..., help="Directory to watch for new files"),
    tiled_uri: str = typer.Option(str, help="Tiled server URI, can bet set with TILED_URI env var"),
    tiled_raw_root: str = typer.Option(str, help= "Root locatiuon of tile raw data can be set with TILED_RAW_ROOT env var"),
    tiled_api_key: Optional[int] = typer.Option(None, help="tiled apikey can be set with TILED_API_KEY env vars"),
    log_level: str = typer.Option(
        "INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    ),
):
    setup_logging(log_level)

    loop = asyncio.get_event_loop()


    if not tiled_uri:
        tiled_uri = os.getenv("TILED_URI")
        if not tiled_api_key:
            tiled_api_key = os.getenv("TILED_API_KEY")
        if not tiled_raw_root:
            tiled_raw_root = os.getenv("TILED_RAW_ROOT")
        
       
        logger.info(f"Connecting to Tiled server at {tiled_uri} with root {tiled_raw_root}")
        tiled_client = from_uri(tiled_uri, api_key=tiled_api_key)
        tiled_ingestor = TiledIngestor(tiled_client, tiled_raw_root, directory)
        redis_client = redis.Redis(host="kvrocks", port=6666, decode_responses=True)
        redis_publisher = RedisPublisher(redis_client, "sas_file_watcher")
        publisher = TiledPublisher(f"{tiled_uri}{tiled_raw_root}", tiled_ingestor, redis_publisher)
        logger.info("Using Tiled publisher")
    else:
        publisher = NullPublisher()
        logger.info("Using default null publisher")

    operator = FileWatcherOperator(publisher)
    listener = FileWatcherListener(str(directory), operator)
    loop.run_until_complete(listener.start())


if __name__ == "__main__":
    app()
