import logging

import msgpack
import zmq
import zmq.asyncio
from arroyopy.listener import Listener
from arroyopy.operator import Operator
from arroyopy.publisher import Publisher
from zmq.asyncio import Context, Socket

from .schemas import (
    SASMessage,
    SASRawEvent,
    SASStart,
    SASStop,
    SerializableNumpyArrayModel,
)

logger = logging.getLogger(__name__)


class ZMQFrameListener(Listener):
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
                    logger.debug(f"Received Start {message}")
                    message = SASStart(**message)
                elif message_type == "event":
                    logger.debug("Received event")
                    # image = SerializableNumpyArrayModel.deserialize_array(
                    #     message["image"]
                    # )
                   
                    message = SASRawEvent(**message)
                elif message_type == "stop":
                    logger.info(f"Received Stop {message}")
                    message = SASStop(**message)
                else:
                    logger.error(f"Unknown message type {message_type}")
                    continue
                await self.operator.process(message)
            except Exception as e:
                logger.exception(f"Error processing message: {e}")

    async def stop(self):
        pass

    @classmethod
    def from_settings(cls, settings: dict, operator: Operator) -> "ZMQFrameListener":
        context = Context()
        zmq_socket = context.socket(zmq.SUB)
        zmq_socket.connect(settings.zmq_address)
        zmq_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        zmq_socket.setsockopt(zmq.SNDHWM, 10000)  # Allow up to 10,000 messages
        zmq_socket.setsockopt(zmq.RCVHWM, 10000)
        logger.info(f"##### Listening for frames on {settings.zmq_address}")
        return cls(operator, zmq_socket)


class ZMQFramePublisher(Publisher):
    def __init__(self, zmq_socket: Socket):
        self.zmq_socket = zmq_socket

    async def publish(self, message: SASMessage) -> None:
        logger.debug(f"Publishing message: {message.msg_type}")
        if isinstance(message, SASStart) or isinstance(message, SASStop):
            message = msgpack.packb(message.model_dump(), use_bin_type=True)
            await self.zmq_socket.send(message)
            return
        if isinstance(message, SASRawEvent):
            message = message.model_dump()
            # message["image"] = SerializableNumpyArrayModel.serialize_array(
            #     message["image"]["array"]
            # )
            message = msgpack.packb(message, use_bin_type=True)
            await self.zmq_socket.send(message)
        else:
            logger.warning(f"Unknown message type: {type(message)}")

    @classmethod
    def from_settings(cls, settings) -> "ZMQFramePublisher":
        context = Context()
        zmq_socket = context.socket(zmq.PUB)
        zmq_socket.bind(settings.address)
        logger.info(f"##### Publishing frames to {settings.address}")
        return cls(zmq_socket)


class ZMQBroker:
    """
    Creates the Dealer in REP-REQ pattern.
    The router listens for requests on the router socket and forwards them to the dealer,
    handling a round robin distribution of requests to workders who subscribe to the dealer.

    """

    def __init__(
        self, zmq_dealer_address: str, zmq_router_address: str, router_hwm: int
    ):
        self.zmq_dealer_address = zmq_dealer_address
        self.zmq_router_address = zmq_router_address
        self.router_hwm = router_hwm

    async def start(self):
        logger.info("Starting ZMQ Dealer and router")
        logger.info(f"Dealer address: {self.zmq_dealer_address}")
        logger.info(f"Router address: {self.zmq_router_address}")
        context = zmq.asyncio.Context()
        router_socket = context.socket(zmq.ROUTER)
        dealte_socket = context.socket(zmq.DEALER)
        dealte_socket.bind(self.zmq_dealer_address)  # Accept request from clients
        router_socket.bind(self.zmq_router_address)  # Distribute requests to workers
        logger.info("Starting Proxy")
        zmq.proxy(router_socket, dealte_socket)

    @classmethod
    def from_settings(cls, settings: dict) -> "ZMQBroker":
        return cls(
            settings.dealer_address, settings.router_address, settings.router_hwm
        )
