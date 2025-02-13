import logging

import msgpack
import zmq
from arroyopy.listener import Listener
from arroyopy.operator import Operator
from arroyopy.publisher import Publisher
from zmq.asyncio import Context, Socket

from .schemas import (
    GISAXSMessage,
    GISAXSRawEvent,
    GISAXSRawStart,
    GISAXSRawStop,
    SerializableNumpyArrayModel,
)

logger = logging.getLogger(__name__)


class ZMQPubSubListener(Listener):
    """
    Takes messages from ZQM and deserializes them into GISAXSMessage objects
    """

    def __init__(self, operator: Operator, zmq_socket: Socket):
        self.operator = operator
        self.zmq_socket = zmq_socket

    async def start(self):
        logger.info("ZMQ Listen loop started")
        while True:
            try:
                raw_msg = await self.zmq_socket.recv()
                message = msgpack.unpackb(raw_msg, raw=False)
                message_type = message.get("msg_type")
                if message_type == "start":
                    logger.info(f"Received Start {message}")
                    message = GISAXSRawStart(**message)
                elif message_type == "event":
                    image = SerializableNumpyArrayModel.deserialize_array(
                        message["image"]
                    )
                    message["image"] = image
                    message = GISAXSRawEvent(**message)
                elif message_type == "stop":
                    logger.info(f"Received Stop {message}")
                    message = GISAXSRawStop(**message)
                else:
                    logger.error(f"Unknown message type {message_type}")
                    continue
                await self.operator.process(message)
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    async def stop(self):
        pass


class ZMQFramePublisher(Publisher):
    async def start(self, zmq_socket: Socket):
        self.zmq_socket = zmq_socket

    async def publish(self, message: GISAXSMessage) -> None:
        if isinstance(message, GISAXSRawStart) or isinstance(message, GISAXSRawStop):
            message = msgpack.packb(message, use_bin_type=True)
            await self.zmq_socket.send(message)
        if isinstance(message, GISAXSRawEvent):
            message = message.dict()
            message["image"] = SerializableNumpyArrayModel.serialize_array(
                message["image"]
            )
            message = msgpack.packb(message, use_bin_type=True)
            await self.zmq_socket.send(message)
        else:
            logger.warning(f"Unknown message type: {type(message)}")

    @classmethod
    def from_settings(cls, settings) -> "ZMQFramePublisher":
        context = Context()
        zmq_socket = context.socket(zmq.PUB)
        zmq_socket.connect(settings.zmq_publish_address)
        return cls(zmq_socket)


class ZMQBroker:
    """
    Creates the Dealer in REP-REQ pattern.
    The router listens for requests on the router socket and forwards them to the dealer,
    handling a round robin distribution of requests to workders who subscribe to the dealer.

    """

    def __init__(self, router_socket: Socket, dealer_socket: Socket, settings: dict):
        self.router_socket = router_socket
        self.dealer_socket = dealer_socket
        self.settings = settings

    def start(self):
        logger.info("Starting ZMQ Dealer and router")
        logger.info(f"Dealer address: {self.settings.zmq_dealer_address}")
        logger.info(f"Router address: {self.settings.zmq_router_address}")
        self.dealer_socket.bind(
            self.settings.zmq_dealer_address
        )  # Accept request from clients
        self.router_socket.bind(
            self.settings.zmq_router_address
        )  # Distribute requests to workers
        zmq.proxy(self.dealer_socket, self.router_socket)

    @classmethod
    def from_settings(cls, settings) -> "ZMQBroker":
        context = Context()
        frontend_router = context.socket(zmq.ROUTER)
        backend_dealer = context.socket(zmq.DEALER)
        return cls(frontend_router, backend_dealer)
