import asyncio
import logging
import os
import sys
import time
from collections import defaultdict

from tiled.client import from_uri
from tiled.client.base import BaseClient
from tiled.client.stream import LiveChildCreated

from arroyosas.tiled.tiled_websocket_bluesky import ChildCreatedEvent, FrameDataEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

subs = []


class TiledClientListener:
    """Subscribes to Tiled websocket events and prints them to stdout."""

    def __init__(
        self,
        tiled_client: BaseClient,
        stream_name: str,
        raw_data_path: str | None = None,
        target: str = "img",
    ) -> None:
        self.tiled_client = tiled_client
        self.stream_name = stream_name
        self.raw_data_path = raw_data_path
        self.target = target
        self.event_counters: defaultdict[str, int] = defaultdict(int)

    def on_new_run(self, event: LiveChildCreated) -> None:
        uid = event.key

        run_node = event.child()
        run_sub = run_node.subscribe()
        logger.info(f"New run: {uid}  scan_id: {run_node.metadata.get('scan_id', 'N/A')}")
        run_sub.child_created.add_callback(self.on_streams_namespace)
        run_sub.start()
        subs.append(run_sub)  # Keep a reference to prevent garbage collection

    def on_streams_namespace(self, event: LiveChildCreated) -> None:
        # if event.key != "streams":
        #     return
        uid = event.key
        logger.info(f"New streams namespace: {uid}")
        streams_sub = event.child().subscribe()
        streams_sub.child_created.add_callback(self.on_new_stream)
        streams_sub.start()
        subs.append(streams_sub)  # Keep a reference to prevent garbage collection

    def on_new_stream(self, event: ChildCreatedEvent) -> None:
        """
        Handle new stream
        """
        logger.debug(f"Handling new stream event: {event.key}") if logger.isEnabledFor(logging.DEBUG) else None

        # if stream_name != self.stream_name:
        #     return

        if self.create_run_logs:
            self.log_message_to_json("on_new_stream", event.subscription, event.model_dump())

        stream_sub = event.child().subscribe()
        stream_sub.child_created.add_callback(self.on_node_in_stream)
        stream_sub.start()
        subs.append(stream_sub)  # Keep a reference to prevent garbage collection

    def on_event(self, event: FrameDataEvent) -> None:
        """
        Handle new event
        """

        logger.debug(f"New event: {event.key}") if logger.isEnabledFor(logging.DEBUG) else None
        if self.create_run_logs:
            self.log_message_to_json("on_event", event.subscription, event.model_dump())

        self.publish_event(event)

    def on_node_in_stream(self, event: ChildCreatedEvent) -> None:
        # Why isn't this firing??

        print(f"!!!!!!!!!!!!!    New node in stream event: {event.key}")
        key = event.key
        logger.debug(f"New node in stream: {key}") if logger.isEnabledFor(logging.DEBUG) else None
        if self.create_run_logs:
            self.log_message_to_json("on_node_in_stream", event.subscription, event.model_dump())

        # Log what we're comparing for debugging
        (logger.debug(f"Checking key '{key}' against target '{self.target}'") if logger.isEnabledFor(logging.DEBUG) else None)

        if key != self.target:
            (
                logger.debug(f"Key '{key}' does not match target '{self.target}', skipping")
                if logger.isEnabledFor(logging.DEBUG)
                else None
            )
            return

        (
            logger.debug(f"Key '{key}' matches target '{self.target}', proceeding")
            if logger.isEnabledFor(logging.DEBUG)
            else None
        )

        stream_sub = event.child().subscribe()
        stream_sub.new_data.add_callback(self.on_event)
        stream_sub.start()

    def _start(self) -> None:
        logger.info("Starting listener")
        node = self.tiled_client[self.raw_data_path]
        catalog_sub = node.subscribe()
        catalog_sub.child_created.add_callback(self.on_new_run)
        catalog_sub.start_in_thread()
        logger.info("Listening...")
        try:
            while True:
                time.sleep(1)  # Prevent busy waiting
        except KeyboardInterrupt:
            logger.info("Stopping listener")
            catalog_sub.stop()  # Stop the subscription to clean up

    async def start(self) -> None:
        """Start the listener in a background thread."""
        await asyncio.to_thread(self._start)


def tiled_ws_listener_factory(
    uri: str,
    stream_name: str,
    raw_data_path: str | None = None,
    target: str = "img",
    api_key: str | None = None,
) -> TiledClientListener:
    """Create a TiledClientListener connected to the given URI.

    Args:
        uri: Tiled server URI.
        stream_name: Name of the stream to subscribe to (e.g. "primary").
        raw_data_path: Path within the catalog to subscribe at.
        target: Key within the stream to listen for frame data.
        api_key: Tiled API key; falls back to TILED_LIVE_API_KEY or TILED_API_KEY env vars.

    Returns:
        A configured TiledClientListener ready to start.
    """
    if api_key is None:
        api_key = os.environ.get("TILED_LIVE_API_KEY") or os.environ.get("TILED_API_KEY")
    client = from_uri(uri, api_key=api_key)
    return TiledClientListener(
        tiled_client=client,
        stream_name=stream_name,
        raw_data_path=raw_data_path,
        target=target,
    )


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    listener = tiled_ws_listener_factory(
        "https://tiled.nsls2.bnl.gov",
        "primary",
        raw_data_path="smi/migration",
    )
    asyncio.run(listener.start())
