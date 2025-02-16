import logging

import msgpack
import zmq
from arroyopy.operator import Operator

from ..schemas import (
    GISAXSLatentSpaceEvent,
    GISAXSMessage,
    GISAXSRawEvent,
    GISAXSStart,
    GISAXSStop,
)

logger = logging.getLogger(__name__)


class LatentSpaceOperator(Operator):
    def __init__(self, proxy_socket: zmq.Socket):
        super().__init__()
        self.proxy_socket = proxy_socket

    async def process(self, message: GISAXSMessage) -> None:
        logger.debug("message recvd")
        if isinstance(message, GISAXSStart):
            logger.info("Received Start Message")
            await self.publish(message)
        elif isinstance(message, GISAXSRawEvent):
            await self.dispatch(message)
            # lse_event = await self.publish(message)
        elif isinstance(message, GISAXSStop):
            logger.info("Received Stop Message")
            await self.publish(message)
        else:
            logger.warning(f"Unknown message type: {type(message)}")
        return None

    async def dispatch(self, message: GISAXSRawEvent) -> GISAXSLatentSpaceEvent:
        try:
            message = message.model_dump()
            message = msgpack.packb(message, use_bin_type=True)
            await self.proxy_socket.send(message)
            logger.debug("sent frame to broker")
            response = await self.proxy_socket.recv()
            logger.debug("response from broker")
            return response
        except Exception as e:
            logger.error(f"Error sending message to broker {e}")

    @classmethod
    def from_settings(cls, settings):
        # Connect to the ZMQ Router/Dealer as a client
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.DEALER)
        socket.setsockopt(zmq.SNDHWM, 10000)  # Allow up to 10,000 messages
        socket.setsockopt(zmq.RCVHWM, 10000)
        socket.connect(settings.zmq_broker.router_address)
        logger.info(f"Connected to broker at {settings.zmq_broker.router_address}")
        return cls(socket)
