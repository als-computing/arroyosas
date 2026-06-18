import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from typing import Any, Dict, TypeAlias

# import numpy as np
from arroyopy.listener import Listener
from tiled.client import from_uri
from tiled.client.base import BaseClient
from tiled.client.stream import LiveArrayData, LiveArrayRef, LiveChildCreated

from arroyosas.schemas import (  # SASStop,; SerializableNumpyArrayModel,
    RawFrameEvent,
    SASMessage,
    SASStart,
    SerializableNumpyArrayModel,  # ← added
)

logger = logging.getLogger(__name__)

ChildCreatedEvent: TypeAlias = LiveChildCreated
FrameDataEvent: TypeAlias = LiveArrayData | LiveArrayRef


class TiledClientListener(Listener):
    """
    A listener that subscribes to Tiled events and processes them
    These subscriptions are used to listen for new runs, streams, and nodes in the Tiled context.
    It handles events by creating subscriptions and invoking callbacks for each event type.

    Subscriptiosns are over web sockets, allowing real-time updates from the Tiled server.
    """

    def __init__(
        self,
        tiled_client: BaseClient,
        stream_name: str,
        raw_data_path: str,
        target: str = "img",
        create_run_logs: bool = True,
        log_dir: str = "tiled_logs",
    ):
        self.tiled_client = tiled_client
        self.stream_name = stream_name
        self.raw_data_path = raw_data_path
        self.target = target
        self.create_run_logs = create_run_logs
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        self.log_dir = log_dir
        self.current_run_dir = None
        self.event_counters = defaultdict(int)  # Track sequence numbers for each event type

    def on_new_run(self, event: ChildCreatedEvent) -> None:
        """
        Handle new run events by creating a subscription for the run.
        """
        uid = event.key
        logger.debug(f"New run {uid}") if logger.isEnabledFor(logging.DEBUG) else None

        # Create new folder for this run
        if self.create_run_logs:
            self.create_run_folder(uid)
            self.log_message_to_json("on_new_run", event.subscription, event.model_dump())

        # Subscribe to the run
        run_node = event.child()
        run_sub = run_node.subscribe(start=0)
        run_sub.child_created.add_callback(self.on_streams_namespace)
        run_sub.start()
        # Publish start event
        self.publish_start(run_node, {"key": uid})

    def on_streams_namespace(self, event: ChildCreatedEvent) -> None:
        """
        Handle new streams namespace events by subscribing to the 'streams' segment.
        For example, this might be the creation of 'baseline' or 'primary' streams.
        """
        logger.debug(event) if logger.isEnabledFor(logging.DEBUG) else None

        if event.key != "streams":
            return

        # Log the event
        if self.create_run_logs:
            self.log_message_to_json("on_streams_namespace", event.subscription, event.model_dump())

        streams_sub = event.child().subscribe(start=0)
        streams_sub.child_created.add_callback(self.on_new_stream)
        streams_sub.start()

    def on_new_stream(self, event: ChildCreatedEvent) -> None:
        """
        Handle new stream
        """
        logger.debug(event) if logger.isEnabledFor(logging.DEBUG) else None
        stream_name = event.key
        (logger.info(f"new stream {stream_name}") if logger.isEnabledFor(logging.INFO) else None)

        if stream_name != self.stream_name:
            return

        if self.create_run_logs:
            self.log_message_to_json("on_new_stream", event.subscription, event.model_dump())

        stream_sub = event.child().subscribe(start=0)
        stream_sub.child_created.add_callback(self.on_node_in_stream)
        stream_sub.start()

    def on_event(self, event: FrameDataEvent) -> None:
        """
        Handle new event
        """

        logger.info(event) if logger.isEnabledFor(logging.INFO) else None
        if self.create_run_logs:
            self.log_message_to_json("on_event", event.subscription, event.model_dump())

        self.publish_event(event)

    def on_node_in_stream(self, event: ChildCreatedEvent) -> None:
        logger.debug(event) if logger.isEnabledFor(logging.DEBUG) else None
        key = event.key

        if self.create_run_logs:
            self.log_message_to_json("on_node_in_stream", event.subscription, event.model_dump())

        # Log what we're comparing for debugging
        (logger.info(f"Checking key '{key}' against target '{self.target}'") if logger.isEnabledFor(logging.INFO) else None)

        if key != self.target:
            (
                logger.info(f"Key '{key}' does not match target '{self.target}', skipping")
                if logger.isEnabledFor(logging.INFO)
                else None
            )
            return

        (logger.info(f"Key '{key}' matches target '{self.target}', proceeding") if logger.isEnabledFor(logging.INFO) else None)

        stream_sub = event.child().subscribe(start=0)
        stream_sub.new_data.add_callback(self.on_event)
        stream_sub.start()

    async def start(self) -> None:
        """Start the listener by calling _start method."""
        self._running = True
        await asyncio.to_thread(self._start)

    def _start(self) -> None:
        """Subscribe to the socket at the provided base segments level"""

        node = self.tiled_client[self.raw_data_path]
        catalog_sub = node.subscribe()
        catalog_sub.child_created.add_callback(self.on_new_run)
        catalog_sub.start()
        print("I'm running")

    async def stop(self) -> None:
        """Stop the listener by calling _stop method."""
        self._running = False
        await super().stop()

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
        """
        Publish an event to the operator.
        """
        message = RawFrameEvent(
            image=SerializableNumpyArrayModel(array=event.data()),
            frame_number=event.sequence,
            tiled_url="",  # Placeholder for actual URL if needed
        )
        self.send_to_operator(message)

    def print_event(self, event_name: str, data: Dict[str, Any]) -> None:
        """Print event information - placeholder method"""
        print(f"Event: {event_name}, Data: {data}")

    def create_run_folder(self, run_id: str) -> str:
        """Create a new folder for the current run"""
        run_folder = os.path.join(self.log_dir, f"run_{run_id}")
        os.makedirs(run_folder, exist_ok=True)
        self.current_run_dir = run_folder
        self.event_counters.clear()  # Reset counters for new run
        return run_folder

    def log_message_to_json(self, event_name: str, sub_data: Any, callback_data: Dict[str, Any]) -> None:
        """Log event data to JSON file with sequence numbering"""
        if self.current_run_dir is None:
            return

        # Increment counter for this event type
        self.event_counters[event_name] += 1
        sequence = self.event_counters[event_name]

        # Create filename with sequence number
        filename = f"{event_name}_{sequence:04d}.json"
        filepath = os.path.join(self.current_run_dir, filename)

        # Prepare data to log
        log_data = {
            "event_name": event_name,
            "sequence": sequence,
            "timestamp": time.time(),
            "subscription_segments": (getattr(sub_data, "segments", None) if hasattr(sub_data, "segments") else None),
            "callback_data": callback_data,
        }

        # Write to JSON file
        try:
            with open(filepath, "w") as f:
                json.dump(log_data, f, indent=2, default=str)
            (logger.debug(f"Logged {event_name} event to {filepath}") if logger.isEnabledFor(logging.DEBUG) else None)
        except Exception as e:
            logger.error(f"Failed to log event {event_name}: {e}")


def tiled_ws_listener_factory(
    uri: str,
    stream_name: str,
    operator=None,
    raw_data_path: str = None,
    target: str = "img",
    create_run_logs: bool = True,
    log_dir: str = "tiled_logs",
    api_key: str = None,
) -> TiledClientListener:
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
    )


if __name__ == "__main__":
    listener = tiled_ws_listener_factory("https://tiled.nsls2.bnl.gov", "primary", raw_data_path="smi/migration")
    asyncio.run(listener.start())
