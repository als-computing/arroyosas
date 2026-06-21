"""Websocket-based Tiled listener for Bluesky runs, streams, and frame events."""

import asyncio
import json
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Any, Dict, TypeAlias

from arroyopy.listener import Listener
from tiled.client import from_uri
from tiled.client.base import BaseClient
from tiled.client.stream import LiveArrayData, LiveArrayRef, LiveChildCreated

from arroyosas.schemas import (
    RawFrameEvent,
    SASMessage,
    SASStart,
    SerializableNumpyArrayModel,
)

logger = logging.getLogger(__name__)

ChildCreatedEvent: TypeAlias = LiveChildCreated
FrameDataEvent: TypeAlias = LiveArrayData | LiveArrayRef

subs = []  # Keep a reference to all subscriptions to prevent garbage collection


class TiledClientListener(Listener):
    """Subscribes to Tiled websocket events and processes Bluesky run data.

    Subscriptions are over websockets, allowing real-time updates from the
    Tiled server as new runs, streams, and frame events appear.

    Args:
        tiled_client: An authenticated Tiled root client.
        stream_name: Name of the stream to watch for frame data (e.g. ``"primary"``).
        raw_data_path: Path within the catalog to subscribe at.
        target: Key within each stream to watch for frame data (e.g. ``"img"``).
        create_run_logs: Whether to write per-event JSON log files.
        log_dir: Root directory for run log folders.
        current_run_dir: Override the active run log directory.
    """

    def __init__(
        self,
        tiled_client: BaseClient,
        stream_name: str,
        raw_data_path: str | None = None,
        target: str = "img",
        create_run_logs: bool = True,
        log_dir: str = "tiled_logs",
        current_run_dir: str | None = None,
    ) -> None:
        self.tiled_client = tiled_client
        self.stream_name = stream_name
        self.raw_data_path = raw_data_path
        self.target = target
        self.create_run_logs = create_run_logs
        if create_run_logs:
            os.makedirs(log_dir, exist_ok=True)
        self.log_dir = log_dir
        self.current_run_dir = current_run_dir
        self.event_counters: defaultdict[str, int] = defaultdict(int)
        self._running = False

    # ------------------------------------------------------------------
    # Websocket callbacks
    # ------------------------------------------------------------------

    def on_new_run(self, event: ChildCreatedEvent) -> None:
        """Handle new run events by subscribing to the run node."""
        uid = event.key

        if self.create_run_logs:
            self.create_run_folder(uid)
            self.log_message_to_json("on_new_run", event.subscription, event.model_dump())

        run_node = event.child()
        logger.info("New run: %s  scan_id: %s", uid, run_node.metadata.get("scan_id", "N/A"))
        run_sub = run_node.subscribe(start=0)
        run_sub.child_created.add_callback(self.on_streams_namespace)
        run_sub.start_in_thread()
        subs.append(run_sub)

        self.publish_start(run_node, {"key": uid})

    def on_streams_namespace(self, event: ChildCreatedEvent) -> None:
        """Handle new namespace events; only descends into the 'streams' namespace."""
        logger.debug("on_streams_namespace: %s", event)

        if event.key != "streams":
            return

        if self.create_run_logs:
            self.log_message_to_json("on_streams_namespace", event.subscription, event.model_dump())

        streams_sub = event.child().subscribe(start=0)
        streams_sub.child_created.add_callback(self.on_new_stream)
        streams_sub.start_in_thread()
        subs.append(streams_sub)

    def on_new_stream(self, event: ChildCreatedEvent) -> None:
        """Handle new stream events; only descends into the configured stream_name."""
        logger.debug("on_new_stream: %s", event.key)

        if event.key != self.stream_name:
            return

        if self.create_run_logs:
            self.log_message_to_json("on_new_stream", event.subscription, event.model_dump())

        stream_sub = event.child().subscribe(start=0)
        stream_sub.child_created.add_callback(self.on_node_in_stream)
        stream_sub.start_in_thread()
        subs.append(stream_sub)

    def on_node_in_stream(self, event: ChildCreatedEvent) -> None:
        """Handle new nodes inside a stream; only subscribes to the target key."""
        key = event.key
        logger.debug("on_node_in_stream: %s (target: %s)", key, self.target)

        if self.create_run_logs:
            self.log_message_to_json("on_node_in_stream", event.subscription, event.model_dump())

        if key != self.target:
            return

        stream_sub = event.child().subscribe(start=0)
        stream_sub.new_data.add_callback(self.on_event)
        stream_sub.start()
        subs.append(stream_sub)

    def on_event(self, event: FrameDataEvent) -> None:
        """Handle new frame data events."""
        logger.debug("on_event: %s", event)

        if self.create_run_logs:
            self.log_message_to_json("on_event", event.subscription, event.model_dump())

        self.publish_event(event)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the websocket listener in a background thread."""
        self._running = True
        await asyncio.to_thread(self._start)

    def _start(self) -> None:
        logger.info("Starting TiledClientListener")
        node = self.tiled_client[self.raw_data_path]
        catalog_sub = node.subscribe()
        catalog_sub.child_created.add_callback(self.on_new_run)
        catalog_sub.start_in_thread()
        logger.info("Listening for websocket events...")
        try:
            while True:
                time.sleep(1)
                if not self._running:
                    logger.info("Stopping listener")
                    for sub in subs:
                        sub.stop()
                    break
        except KeyboardInterrupt:
            logger.info("Interrupted, stopping listener")
            catalog_sub.stop()
        except Exception as e:
            logger.error("Listener error: %s", e)
            for sub in subs:
                sub.stop()

    async def stop(self) -> None:
        """Stop the listener."""
        self._running = False
        await super().stop()

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def send_to_operator(self, message: SASMessage) -> None:
        asyncio.run(self.publish(message))

    def publish_start(self, run_node: BaseClient, data: Dict[str, Any]) -> None:
        metadata = getattr(run_node, "metadata", {}) or {}
        base_url = getattr(self.tiled_client.context, "base_url", "")
        start = SASStart(
            run_id=data["key"],
            run_name=metadata.get("run_name", data["key"]),
            width=metadata.get("width", 0),
            height=metadata.get("height", 0),
            data_type=metadata.get("data_type", ""),
            tiled_url=str(base_url),
        )
        self.send_to_operator(start)

    def publish_event(self, event: FrameDataEvent) -> None:
        message = RawFrameEvent(
            image=SerializableNumpyArrayModel(array=event.data()),
            frame_number=event.sequence,
            tiled_url="",
        )
        self.send_to_operator(message)

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def create_run_folder(self, run_id: str) -> str:
        run_folder = os.path.join(self.log_dir, f"run_{run_id}")
        os.makedirs(run_folder, exist_ok=True)
        self.current_run_dir = run_folder
        self.event_counters.clear()
        return run_folder

    def print_event(self, event_name: str, data: Dict[str, Any]) -> None:
        print(f"Event: {event_name}, Data: {data}")

    def log_message_to_json(self, event_name: str, sub_data: Any, callback_data: Dict[str, Any]) -> None:
        """Write a JSON log entry for *event_name* into the current run directory."""
        if self.current_run_dir is None:
            return

        self.event_counters[event_name] += 1
        sequence = self.event_counters[event_name]
        filename = f"{event_name}_{sequence:04d}.json"
        filepath = os.path.join(self.current_run_dir, filename)
        os.makedirs(self.current_run_dir, exist_ok=True)

        log_data = {
            "event_name": event_name,
            "sequence": sequence,
            "timestamp": time.time(),
            "subscription_segments": (getattr(sub_data, "segments", None) if hasattr(sub_data, "segments") else None),
            "callback_data": callback_data,
        }
        try:
            with open(filepath, "w") as f:
                json.dump(log_data, f, indent=2, default=str)
            logger.debug("Logged %s to %s", event_name, filepath)
        except Exception as e:
            logger.error("Failed to log event %s: %s", event_name, e)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def tiled_ws_listener_factory(
    uri: str,
    stream_name: str,
    operator=None,
    raw_data_path: str | None = None,
    target: str = "img",
    create_run_logs: bool = True,
    log_dir: str = "tiled_logs",
    api_key: str | None = None,
    current_run_dir: str | None = None,
) -> TiledClientListener:
    """Create a TiledClientListener connected to the given URI.

    Args:
        uri: Tiled server URI.
        stream_name: Stream to watch for frame data (e.g. ``"primary"``).
        operator: Unused; kept for call-site compatibility.
        raw_data_path: Path within the catalog to subscribe at.
        target: Key within each stream to watch for frame data.
        create_run_logs: Whether to write per-event JSON log files.
        log_dir: Root directory for run log folders.
        api_key: Tiled API key; falls back to TILED_LIVE_API_KEY or TILED_API_KEY env vars.
        current_run_dir: Override the active run log directory.

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
        create_run_logs=create_run_logs,
        log_dir=log_dir,
        current_run_dir=current_run_dir,
    )


# ---------------------------------------------------------------------------
# __main__
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("numexpr").setLevel(logging.WARNING)
    logger.setLevel(logging.DEBUG)

    listener = tiled_ws_listener_factory(
        uri="https://tiled.nsls2.bnl.gov",
        stream_name="primary",
        raw_data_path="smi/migration",
    )
    asyncio.run(listener.start())
