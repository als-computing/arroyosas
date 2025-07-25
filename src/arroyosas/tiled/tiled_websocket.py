import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from typing import Any, Dict

from arroyosas.schemas import (  # SASStop,; SerializableNumpyArrayModel,
    RawFrameEvent,
    SASMessage,
    SASStart,
)
from arroyopy.listener import Listener
from arroyopy.operator import Operator
from tiled.client import from_uri
from tiled.client.base import BaseClient
from tiled.client.stream import Subscription
from tiled.structures.array import ArrayStructure


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
        stream_name: str = "primary",
        data_source: str = "pil2M_image",
        create_run_logs: bool = False,
        log_dir: str = "tiled_logs",
    ):
        
        self.operator = operator
        self.tiled_client = tiled_client
        self.stream_name = stream_name  # The name of the stream to listen to e.g. 'primary'
        self.data_source = data_source  # The name of the data source to listen to e.g. 'pil2M_image'
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

    def load_data(self, sub, data):
        patch = data['patch']
        logger.debug(data['uri']) if logger.isEnabledFor(logging.DEBUG) else None
        slice_ = tuple(slice(offset, offset + shape) for offset, shape in zip(patch["offset"], patch["shape"]))  # GET /array/full/...
        node = self.tiled_client['/'.join(sub.segments)]  # GET /metadata/... wasteful to do it on each load_data call
        images = node.read(slice=slice_)  # could be sub.node.read(...)
        logger.debug(f"images shape {images.shape}")  if logger.isEnabledFor(logging.DEBUG) else None

    def on_node_in_stream(self, sub, data):
        logger.debug(data) if logger.isEnabledFor(logging.DEBUG) else None
        key = data["key"]

        if self.create_run_logs:
            self.log_message_to_json("on_node_in_stream", sub, data)

        # Log what we're comparing for debugging
        logger.info(
            f"Checking key '{key}'"
        ) if logger.isEnabledFor(logging.INFO) else None

        logger.info(
            f"Key '{key}', proceeding"
        ) if logger.isEnabledFor(logging.INFO) else None
        try:
            stream_sub = Subscription(
                self.tiled_client.context, sub.segments + [key], start=0
            )
            stream_sub.add_callback(self.load_data)
            stream_sub.start()
            self.publish_event(data)
        except Exception as e:
            logger.error(f"Error processing node {sub.segments + [key]}: {e}")

    async def start(self) -> None:
        """Start the listener by calling _start method."""
        self._running = True
        await asyncio.to_thread(self._start)
        while True:
            if not self._running:
                break
            await asyncio.sleep(1)

    def _start(self) -> None:
        """
        Subscribe to the socket at the provided base segments level

        When tiledwriter puts bluesky data into tiled, it's in newish tree:
        catalog (those in quotes are literal names):

            - run:  (run start and stop as metadata)
                - "streams": (namespace)
                    - stream_name: (e.g. 'primary')
                        - key: (e.g. 'pil2M_image')
                        - "internal" (table of data from events)
        """

        node = self.tiled_client
        catalog_sub = Subscription(node.context)
        catalog_sub.add_callback(self.on_new_run)
        # catalog_sub.add_callback(self.test)
        try:
            catalog_sub.start()
        except Exception as e:
            # self.context.revoke_api_key(key_info["first_eight"])

            return
        print("I'm running")

    async def stop(self) -> None:
        """Stop the listener by calling _stop method."""
        self._running = False
        await super().stop()

    def send_to_operator(self, message: SASMessage) -> None:
        asyncio.run(self.operator.process(message))

    def publish_start(self, data: Dict[str, Any]) -> None:

        # We need to make a request to get image information
        
        # structure = ArrayStructure.from_json(data["data_source"]["structure"])
        structure = self.tiled_client[data['key']]['streams'][self.stream_name][self.data_source]._structure
        start = SASStart(
            run_name=data['key'],
            run_id=data['key'],
            width=structure.shape[1],
            height=structure.shape[2],
            data_type=structure.data_type.to_numpy_dtype().str,
            tiled_url=self.tiled_client.uri,
        )
        logging.debug(f"sending start message: {start}") if logging.getLogger().isEnabledFor(logging.DEBUG) else None
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

        # Create log directory if specified in settings
        log_dir = getattr(settings, "log_dir", "tiled_logs")

        return cls(
            op,
            client,
            settings.stream_name,
            settings.data_source,
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
        "log_dir": "tiled_event_logs",  # Directory for JSON log
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
