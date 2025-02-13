import logging

from arroyopy.operator import Operator

from ..schemas import GISAXSMessage, GISAXSRawEvent, GISAXSRawStart, GISAXSRawStop

logger = logging.getLogger(__name__)


class LatentSpaceOperator(Operator):
    def __init__(self):
        super().__init__()

    async def process(self, message: GISAXSMessage) -> None:
        if isinstance(message, GISAXSRawStart):
            await self.publish(message)
        elif isinstance(message, GISAXSRawEvent):
            await self.publish(message)
        elif isinstance(message, GISAXSRawStop):
            await self.publish(message)
        else:
            logger.warning(f"Unknown message type: {type(message)}")
        return None
