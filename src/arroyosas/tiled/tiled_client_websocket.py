import asyncio
import logging
import time
from typing import Any, Dict, List

# import numpy as np
from arroyopy.listener import Listener
from arroyopy.operator import Operator
from tiled.client import from_uri
from tiled.client.base import BaseClient
from tiled.client.stream import Subscription

from arroyosas.schemas import (  # SASStop,; SerializableNumpyArrayModel,
    RawFrameEvent,
    SASMessage,
    SASStart,
)

logger = logging.getLogger(__name__)


class TiledClientListener(Listener):
    def __init__(
        self,
        operator: Operator,
        tiled_client: BaseClient,
        base_segements: List[str],
        stream_name: str,
        target: str = "img",
    ):
        self.operator = operator
        self.tiled_client = tiled_client
        self.base_segments = base_segements
        self.stream_name = stream_name
        self.target = target

    def on_new_run(self, sub, data):
        uid = data["key"]
        logger.debug(f"New run {uid}")
        run_sub = Subscription(self.tiled_client.context, [uid], start=0)
        run_sub.add_callback(self.on_streams_namespace)
        run_sub.start()

    def on_streams_namespace(self, sub, data):
        logger.debug(data)
        streams_sub = Subscription(
            self.tiled_client.context, sub.segments + ["streams"], start=0
        )
        streams_sub.add_callback(self.on_new_stream)
        streams_sub.start()

    def on_new_stream(self, sub, data):
        logger.debug(data)
        stream_name = data["key"]
        logger.debug(f"new stream {stream_name}")
        stream_sub = Subscription(
            self.tiled_client.context, sub.segments + [stream_name], start=0
        )
        stream_sub.add_callback(self.on_node_in_stream)
        stream_sub.start()

    def on_node_in_stream(self, sub, data):
        logger.debug(data)
        key = data["key"]
        if key != self.target:
            return
        stream_sub = Subscription(
            self.tiled_client.context, sub.segments + [key], start=0
        )
        stream_sub.add_callback(print)
        stream_sub.start()

    async def start(self) -> None:
        """Start the listener by calling _start method."""
        self._running = True
        await asyncio.to_thread(self._start)

    def _start(self) -> None:
        catalog_sub = Subscription(self.tiled_client.context)
        catalog_sub.add_callback(self.on_new_run)
        catalog_sub.start()

    async def stop(self) -> None:
        """Stop the listener by calling _stop method."""
        self._running = False
        await super().stop()

    def send_to_operator(self, message: SASMessage) -> None:
        asyncio.run(self.operator.process(message))

    def publish_start(self, data: Dict[str, Any]) -> None:
        start = SASStart()
        self.send_to_operator(start)

    def publish_event(self, data: Dict[str, Any]) -> None:
        event = RawFrameEvent(
            data=data,
            segments=self.tiled_frame_segments,
        )
        self.send_to_operator(event)

    @classmethod
    def from_settings(cls, settings: Any, operator: Operator) -> "TiledClientListener":
        """Create a TiledClientListener from settings."""
        client = from_uri(
            settings.uri,
            api_key=settings.api_key,
        )

        logger.info(f"#### Listening for runs at {settings.base_segments}")
        logger.info(f"#### Frames segments: {settings.frames_segments}")

        return cls(
            operator,
            client[settings.base_segments],
            settings.base_segments,
            settings.stream_name,
            settings.target,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # Example usage
    settings = {
        "uri": "http://localhost:8000",
        "api_key": "secret",
        # 'base_segments': ['smi', 'raw'],
        "base_segments": [],
        "frames_segments": ["primary", "data"],
        "stream_name": "pi21M_image",
        "target": "img",
    }

    class Settings:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    settings = Settings(**settings)

    class NullOperator(Operator):
        async def process(self, message: SASMessage) -> None:
            # Dummy process method for demonstration
            logger.info(f"Processing message: {message}")

    operator = NullOperator()  # Replace with actual operator instance
    listener = TiledClientListener.from_settings(settings, operator)

    asyncio.run(listener.start())

    while True:
        time.sleep(5)  # Keep the script running
