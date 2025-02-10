import logging

import msgpack
from arroyopy.listener import Listener
from arroyopy.operator import Operator
from arroyopy.publisher import Publisher
from zmq.asyncio import Socket

from .schemas import (
    GISAXSMessage,
    GISAXSRawEvent,
    GISAXSRawStart,
    GISAXSRawStop,
    SerializableNumpyArrayModel,
)

logger = logging.getLogger(__name__)


class ZMQListener(Listener):
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
    async def start(self):
        pass

    async def publish(self, message: GISAXSMessage) -> None:
        pass
