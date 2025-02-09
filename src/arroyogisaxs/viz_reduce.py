import logging

from arroyopy.operator import Operator

from .schemas import (
    GISAXSMessage,
    GISAXSRawEvent,
    GISAXSReducedData,
    GISAXSStart,
    GISAXSStop,
)

# from .shared_settings import KEY_CURRENT_REDUCTION_PARAMS, SharedSettings
from .shared_settings import SharedSettings

logger = logging.getLogger(__name__)


class GISAXSVizReduceOperator(Operator):
    def __init__(self, shared_settings: SharedSettings):
        super().__init__()
        self.shared_settings = shared_settings

    async def process(self, message: GISAXSMessage) -> GISAXSReducedData:
        if isinstance(message, GISAXSStart):
            await self.publish(message)
        elif isinstance(message, GISAXSRawEvent):
            await self.publish(message)
        elif isinstance(message, GISAXSStop):
            await self.publish(message)
        else:
            logger.warning(f"Unknown message type: {type(message)}")
        return None


def reduce_to_1d(
    message: GISAXSRawEvent, shared_settings: SharedSettings
) -> GISAXSReducedData:
    pass
    # settings_json = shared_settings.get_json(KEY_CURRENT_REDUCTION_PARAMS)
    # image = message.image
