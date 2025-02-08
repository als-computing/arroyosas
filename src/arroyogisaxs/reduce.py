import logging

from arroyopy.operator import Operator

from .schemas import GISAXSEvent, GISAXSMessage, GISAXSStart, GISAXSStop

logger = logging.getLogger(__name__)


class GISAXSOperator(Operator):
    def __init__(self):
        super().__init__()

    async def process(self, message: GISAXSMessage) -> None:
        if isinstance(message, GISAXSStart):
            await self.publish(message)
        elif isinstance(message, GISAXSEvent):
            await self.publish(message)
        elif isinstance(message, GISAXSStop):
            await self.publish(message)
        else:
            logger.warning(f"Unknown message type: {type(message)}")
        return None


# def reduce_to_1d(message: GISAXSEvent):
#     image = message.image
