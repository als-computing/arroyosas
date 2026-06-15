import asyncio
import logging
import os
from typing import Any, Dict

# import numpy as np
from arroyopy.listener import Listener
from tiled.client import from_uri
from tiled.client.base import BaseClient
from tiled.client.stream import Subscription

from arroyosas.schemas import (
    RawFrameEvent,
    SASMessage,
    SASStart,
    SerializableNumpyArrayModel,
)

logger = logging.getLogger(__name__)


class TiledClientListener(Listener):
    """
    A listener that subscribes to Tiled events and processes them
    These subscriptions are used to listen for new runs, streams, and nodes in the Tiled context.
    It handles events by creating subscriptions and invoking callbacks for each event type.

    Subscriptiosns are over web sockets, allowing real-time updates from the Tiled server.
    """

    def __init__(
        self,
        operator,
        tiled_client: BaseClient,
        sub_path: str,
    ):
        self.tiled_client = tiled_client
        self.sub_path = sub_path
        super().__init__(operator)  # Operator will be set later when creating the listener

    async def start(self) -> None:
        """Start the listener by calling _start method."""
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        await asyncio.to_thread(self._start)
        await self._stop_event.wait()

    def _start(self) -> None:
        try:
            """Subscribe to the socket at the provided base segments level"""
            node = self.tiled_client[self.sub_path]
            catalog_sub = Subscription(node.context, node.path_parts)
            catalog_sub.add_callback(self.on_new_data_collection)
            catalog_sub.start()
            (
                logger.debug("TiledClientListener started and subscribed to base segments")
                if logger.isEnabledFor(logging.DEBUG)
                else None
            )
        except Exception as e:
            logger.error(f"Error starting TiledClientListener: {e}")

    def on_new_data_collection(self, sub: Subscription, data: Dict[str, Any]):
        """
        Handle new data collection events by creating a subscription for the data collection.
        For 7.3.3, we assume all runs go into a single collection. A run is a collection and contains
        0 or more ArrayClients and maybe a Table or two.
        """
        try:
            (logger.info(f"New data collection {data}") if logger.isEnabledFor(logging.INFO) else None)

            # # Subscribe to the run
            if data.get("key") is None:
                # Does only happen once?
                logger.warning("Received on_new_data_collection event without 'key' in data, skipping")
                return
            run_sub = Subscription(self.tiled_client.context, sub.segments + [data["key"]])
            run_sub.add_callback(self.on_new_data_item)
            run_sub.start()
            # Publish start event
            self.publish_start(run_sub, data)
        except Exception as e:
            logger.exception(f"Error in on_new_data_collection: {e}")

    def on_new_data_item(self, sub, data):
        """
        Handle new data item. This is called when a new node is added to the run. We check if it's a stream node and if so, we subscribe to it.
        For 7.3.3, we assume the stream node is directly under the run node.
        """
        try:
            logger.debug(data) if logger.isEnabledFor(logging.DEBUG) else None
            if data.get("key") is None:
                # Does only happen once?
                logger.warning("Received on_new_data_item event without 'key' in data, skipping")
                return
            data_name = data["key"]
            (logger.info(f"new stream {data_name}") if logger.isEnabledFor(logging.INFO) else None)

            self.publish_event(sub, data)
        except Exception as e:
            logger.exception(f"Error in on_new_data_item: {e}")

    async def stop(self) -> None:
        """Stop the listener by calling _stop method."""
        self._running = False
        if hasattr(self, "_stop_event"):
            self._stop_event.set()
        await super().stop()

    def send_to_operator(self, message: SASMessage) -> None:
        try:
            future = asyncio.run_coroutine_threadsafe(self.operator.notify(message), self._loop)
            future.result()
        except Exception as e:
            logger.error(f"Error sending message to operator: {e}")

    def publish_start(self, sub: Subscription, data: Dict[str, Any]) -> None:
        run_key = data["key"]
        run_node = self.tiled_client["/".join(sub.segments)]
        meta = run_node.metadata
        start = SASStart(
            run_id=run_key,
            run_name=meta.get("run_name", run_key),
            width=meta.get("width", 0),
            height=meta.get("height", 0),
            data_type=meta.get("data_type", ""),
            tiled_url=str(self.tiled_client.context.base_url),
        )
        self.send_to_operator(start)

    def publish_event(self, sub: Subscription, data: Dict[str, Any]) -> None:
        """
        Publish an event to the operator.
        """
        data_node = self.tiled_client["/".join(sub.segments + [data["key"]])]
        event = RawFrameEvent(
            image=SerializableNumpyArrayModel(
                array=data_node[:]
            ),  # Assuming the data node is an array-like object. Adjust as needed.
            frame_number=data.get("sequence", 0),
            tiled_url="",  # Placeholder for actual URL if needed
        )
        self.send_to_operator(event)


def tiled_ws_listener_factory(
    uri: str,
    sub_path: str,
    operator=None,
    api_key: str = None,
) -> TiledClientListener:
    if api_key is None:
        api_key = os.environ.get("TILED_LIVE_API_KEY")
    client = from_uri(uri, api_key=api_key)
    return TiledClientListener(operator, client, sub_path)
