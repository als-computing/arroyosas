import logging

from arroyopy.operator import Operator

from ..kv_store import KVStore
from ..schemas import GISAXS1DReduction, GISAXSRawEvent, GISAXSStart, GISAXSStop
from .reduce import pixel_roi_horizontal_cut

logger = logging.getLogger(__name__)


class OneDReductionOperator(Operator):
    def __init__(self, kv_store: KVStore):
        super().__init__()
        self.kv_store = kv_store

    async def process(self, message):
        if isinstance(message, GISAXSStart):
            logger.info(f"Processing Start {message}")
            self.current_reduction_settings = get_reduction_settings()
            logger.info("")
            await self.publish(message)

        if isinstance(message, GISAXSStop):
            logger.info(f"Processing Stop {message}")
            await self.publish(message)

        if isinstance(message, GISAXSRawEvent):
            # For now
            reduction = pixel_roi_horizontal_cut(**self.current_reduction_settings)
            reduction_msg = GISAXS1DReduction(
                curve=reduction,
                curve_tiled_url=self.current_run_url,
                raw_frame=message.image,
                raw_frame_tiled_url=self.current_run_url,
            )
            await self.publish(reduction_msg)

    @classmethod
    def create(cls, kv_store: KVStore) -> "OneDReductionOperator":
        return cls(kv_store)


def get_reduction_settings() -> dict:
    return dict()
