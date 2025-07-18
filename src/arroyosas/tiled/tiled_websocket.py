import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from typing import Any, Dict

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
    """
    A listener that subscribes to Tiled events and processes them
    These subscriptions are used to listen for new runs, streams, and nodes in the Tiled context.
    It handles events by creating subscriptions and invoking callbacks for each event type.

    Subscriptiosns are over web sockets, allowing real-time updates from the Tiled server.
    """

    def __init__(
        self,
        operator: Operator,
        tiled_client: BaseClient,
        stream_name: str,
        target: str = "img",
        create_run_logs: bool = True,
        log_dir: str = "tiled_logs",
    ):
        self.operator = operator
        self.tiled_client = tiled_client
        self.stream_name = stream_name
        self.target = target
        self.create_run_logs = create_run_logs
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        self.log_dir = log_dir
        self.current_run_dir = None
        self.event_counters = defaultdict(
            int
        )  # Track sequence numbers for each event type

    def on_new_run(self, sub: Subscription, data: Dict[str, Any]):
        """
        Handle new run events by creating a subscription for the run.
        """
        uid = data["key"]
        logger.debug(f"New run {uid}") if logger.isEnabledFor(logging.DEBUG) else None

        # Create new folder for this run
        if self.create_run_logs:
            self.create_run_folder(uid)
            self.log_message_to_json("on_new_run", sub, data)

        # Subscribe to the run
        run_sub = Subscription(self.tiled_client.context, [uid], start=0)
        run_sub.add_callback(self.on_streams_namespace)
        run_sub.start()
        # Publish start event
        self.publish_start(data)

    def on_streams_namespace(self, sub, data):
        """
        Handle new streams namespace events by subscribing to the 'streams' segment.
        For example, this might be the creation of 'baseline' or 'primary' streams.
        """
        logger.debug(data) if logger.isEnabledFor(logging.DEBUG) else None

        # Log the event
        if self.create_run_logs:
            self.log_message_to_json("on_streams_namespace", sub, data)

        streams_sub = Subscription(
            self.tiled_client.context, sub.segments + ["streams"], start=0
        )
        streams_sub.add_callback(self.on_new_stream)
        streams_sub.start()

    def on_new_stream(self, sub, data):
        """
        Handle new stream
        """
        logger.debug(data) if logger.isEnabledFor(logging.DEBUG) else None
        stream_name = data["key"]
        logger.info(f"new stream {stream_name}") if logger.isEnabledFor(
            logging.INFO
        ) else None

        if self.create_run_logs:
            self.log_message_to_json("on_new_stream", sub, data)

        stream_sub = Subscription(
            self.tiled_client.context, sub.segments + [stream_name], start=0
        )
        stream_sub.add_callback(self.on_node_in_stream)
        stream_sub.start()

    def on_event(self, sub: Subscription, data: Dict[str, Any]) -> None:
        """
        Handle new event
        """

        logger.info(data) if logger.isEnabledFor(logging.INFO) else None
        if self.create_run_logs:
            self.log_message_to_json("on_event", sub, data)

    def on_node_in_stream(self, sub, data):
        logger.debug(data) if logger.isEnabledFor(logging.DEBUG) else None
        key = data["key"]

        if self.create_run_logs:
            self.log_message_to_json("on_node_in_stream", sub, data)

        # Log what we're comparing for debugging
        logger.info(
            f"Checking key '{key}' against target '{self.target}'"
        ) if logger.isEnabledFor(logging.INFO) else None

        if key != self.target:
            logger.info(
                f"Key '{key}' does not match target '{self.target}', skipping"
            ) if logger.isEnabledFor(logging.INFO) else None
            return

        logger.info(
            f"Key '{key}' matches target '{self.target}', proceeding"
        ) if logger.isEnabledFor(logging.INFO) else None

        stream_sub = Subscription(
            self.tiled_client.context, sub.segments + [key], start=0
        )
        # stream_sub.add_callback(print)
        stream_sub.add_callback(self.on_event)
        stream_sub.start()
        self.publish_event(data)

    async def start(self) -> None:
        """Start the listener by calling _start method."""
        self._running = True
        await asyncio.to_thread(self._start)

    def _start(self) -> None:
        """Subscribe to the socket at the provided base segments level"""

        node = self.tiled_client
        catalog_sub = Subscription(node.context)
        catalog_sub.add_callback(self.on_new_run)
        # catalog_sub.add_callback(self.test)
        catalog_sub.start()
        print("I'm running")

    async def stop(self) -> None:
        """Stop the listener by calling _stop method."""
        self._running = False
        await super().stop()

    def send_to_operator(self, message: SASMessage) -> None:
        asyncio.run(self.operator.process(message))

    def publish_start(self, data: Dict[str, Any]) -> None:
        start = SASStart(
            data=data,  # Include any relevant data for the start event
        )
        self.send_to_operator(start)

    def publish_event(self, data: Dict[str, Any]) -> None:
        """
        Publish an event to the operator.
        """
        event = RawFrameEvent(
            image=None,
            frame_number=data.get("sequence", 0),
            tiled_url="",  # Placeholder for actual URL if needed
        )
        self.send_to_operator(event)

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

    def log_message_to_json(
        self, event_name: str, sub_data: Any, callback_data: Dict[str, Any]
    ) -> None:
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
            "subscription_segments": getattr(sub_data, "segments", None)
            if hasattr(sub_data, "segments")
            else None,
            "callback_data": callback_data,
        }

        # Write to JSON file
        try:
            with open(filepath, "w") as f:
                json.dump(log_data, f, indent=2, default=str)
            logger.debug(
                f"Logged {event_name} event to {filepath}"
            ) if logger.isEnabledFor(logging.DEBUG) else None
        except Exception as e:
            logger.error(f"Failed to log event {event_name}: {e}")

    @classmethod
    def from_settings(cls, settings: Any, op: Operator) -> "TiledClientListener":
        """Create a TiledClientListener from settings."""
        client = from_uri(
            settings.uri,
            api_key=settings.api_key,
        )
        # for key in client.context.whoami()['api_keys']:
        #     client.context.revoke_api_key(key['first_eight'])
        logger.info(f"#### Listening for runs at {settings.base_segments}")
        # logger.info(f"#### Frames segments: {settings.frames_segments}")

        # Create log directory if specified in settings
        log_dir = getattr(settings, "log_dir", "tiled_logs")

        return cls(
            op,
            client,
            settings.stream_name,
            settings.target,
            log_dir,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Example usage
    settings = {
        # "uri": "http://localhost:8000",
        "uri": "https://tiled-dev.nsls2.bnl.gov/",
        "api_key": None,  # Replace with actual API key if needed
        "base_segments": [],
        # "frames_segments": ["primary", "data"],
        "stream_name": "primary",
        "target": "pil2M_image",
        "log_dir": "tiled_event_logs",  # Directory for JSON logs
    }

    class Settings:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    settings = Settings(**settings)

    class NullOperator(Operator):
        async def process(self, message: SASMessage) -> None:
            # Dummy process method for demonstration
            logger.info(f"Processing message: {message}")

    n_operator = NullOperator()  # Replace with actual operator instance
    listener = TiledClientListener.from_settings(settings, n_operator)

    asyncio.run(listener.start())

    while True:
        time.sleep(5)  # Keep the script running
