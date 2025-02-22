import logging

import msgpack
import zmq
from arroyopy.operator import Operator

from ..lse.lse_reducer import LatentSpaceReducer
from ..schemas import (
    GISAXSLatentSpaceEvent,
    GISAXSMessage,
    GISAXSRawEvent,
    GISAXSStart,
    GISAXSStop,
)

logger = logging.getLogger(__name__)


class LatentSpaceOperator(Operator):
    def __init__(self, proxy_socket: zmq.Socket, reducer: LatentSpaceReducer = None):
        super().__init__()
        self.proxy_socket = proxy_socket
        self.reducer = reducer

    async def process(self, message: GISAXSMessage) -> None:
        logger.debug("message recvd")
        if isinstance(message, GISAXSStart):
            logger.info("Received Start Message")
            await self.publish(message)
        elif isinstance(message, GISAXSRawEvent):
            result = await self.dispatch(message)
            await self.publish(result)
        elif isinstance(message, GISAXSStop):
            logger.info("Received Stop Message")
            await self.publish(message)
        else:
            logger.warning(f"Unknown message type: {type(message)}")
        return None

    async def dispatch_broker(self, message: GISAXSRawEvent) -> GISAXSLatentSpaceEvent:
        try:
            message = message.model_dump()
            logger.debug("sending frame to broker")
            message = msgpack.packb(message, use_bin_type=True)
            await self.proxy_socket.send(message)
            # logger.debug("sent frame to broker")
            response = await self.proxy_socket.recv()
            if response == b"ERROR":
                logger.debug("Worker reported an error")
                return None
            # logger.debug("response from broker")
            return GISAXSLatentSpaceEvent(**msgpack.unpackb(response))
        except Exception as e:
            logger.error(f"Error sending message to broker {e}")

    async def dispatch(self, message: GISAXSRawEvent) -> GISAXSLatentSpaceEvent:
        try:
            message = message.model_dump()
            logger.debug("sending frame to broker")
            message = msgpack.packb(message, use_bin_type=True)
            latent_space = self.reducer.reduce(message)
            return GISAXSLatentSpaceEvent(latent_space)
        except Exception as e:
            logger.error(f"Error sending message to broker {e}")
 
    @classmethod
    def from_settings(cls, settings, broker_settings=None):

        # Connect to the ZMQ Router/Dealer as a client
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.SNDHWM, 10000)  # Allow up to 10,000 messages
        socket.setsockopt(zmq.RCVHWM, 10000)
        logger.info(f"Connecting to broker at {settings.zmq_broker.router_address}")
        socket.connect(settings.zmq_broker.router_address)
        logger.info(f"Connected to broker at {settings.zmq_broker.router_address}")
        reducer = None
        if broker_settings:
            reducer = LatentSpaceReducer(
                broker_settings.current_latent_space,
                broker_settings.current_dim_reduction,
                broker_settings.models,
            )
        return cls(socket, reducer)
