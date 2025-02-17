import logging

from arroyopy.operator import Operator

from ..kv_store import KVStore
from ..schemas import GISAXS1DReduction, GISAXSRawEvent, GISAXSStart, GISAXSStop
from .detector import VerticalPilatus900kw
from .reduce import pixel_roi_horizontal_cut

logger = logging.getLogger(__name__)

REDUCTION_CONFIG_KEY = "reduction_config"


class OneDReductionOperator(Operator):
    def __init__(self, kv_store: KVStore):
        super().__init__()
        self.kv_store = kv_store
        self.current_scan_metadata = None
        self.mask = None

    async def process(self, message):
        if isinstance(message, GISAXSStart):
            logger.info(f"Processing Start {message}")
            self.current_scan_metadata = message
            logger.info("Calculating mask")
            reduction_settings = self.kv_store.get_json(REDUCTION_CONFIG_KEY)
            beamstop = (
                reduction_settings.get("beamcenter_x"),
                reduction_settings.get("beamcenter_y"),
            )
            self.mask = VerticalPilatus900kw().calc_mask(beamstop).astype(int)
            await self.publish(message)

        if isinstance(message, GISAXSStop):
            logger.info(f"Processing Stop {message}")
            self.current_scan_metadata = None
            self.current_reduction_settings = None
            await self.publish(message)

        if isinstance(message, GISAXSRawEvent):
            reduction_settings = self.kv_store.get_json(REDUCTION_CONFIG_KEY)
            if reduction_settings is None:
                logger.error("No reduction settings found")
                return
            reduction_settings.pop("input_uri_data")
            reduction_settings.pop("input_uri_mask")
            masked_image = message.image.array + self.mask
            reduction_settings["masked_image"] = masked_image
            reduction = pixel_roi_horizontal_cut(**reduction_settings)
            #
            reduction_msg = GISAXS1DReduction(
                curve=reduction[
                    0
                ],  # just the qparrallel, not the cut_average or errors
                curve_tiled_url=self.current_run_url,
                raw_frame=message.image,
                raw_frame_tiled_url=message.tiled_url,
            )
            await self.publish(reduction_msg)

    @classmethod
    def create(cls, kv_store: KVStore) -> "OneDReductionOperator":
        return cls(kv_store)
