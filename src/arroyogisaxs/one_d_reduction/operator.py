import asyncio
import logging
import os

from arroyopy.operator import Operator
from tiled.client import from_uri
from tiled.client.base import BaseClient
import numpy as np



from ..redis import RedisConn
from ..schemas import (
    GISAXS1DReduction,
    GISAXSRawEvent,
    GISAXSStart,
    GISAXSStop,
    SerializableNumpyArrayModel,
)
from ..tiled import get_nested_client
from .detector import VerticalPilatus900kw
from .reduce import pixel_roi_horizontal_cut

logger = logging.getLogger(__name__)

REDUCTION_CONFIG_KEY = "reduction_config"
REDUCTION_CHANNEL = "scattering"


class OneDReductionOperator(Operator):
    def __init__(self, tiled_client: BaseClient, redis_conn: RedisConn):
        super().__init__()
        self.tiled_client = tiled_client
        self.redis_conn = redis_conn
        self.current_scan_metadata = None
        #self.mask = None
        self.mask = self.load_static_mask_file()


        asyncio.create_task(
            self.redis_conn.redis_subscribe(REDUCTION_CHANNEL, self.compute_callback)
        )

    async def process(self, message):
        try:
            if isinstance(message, GISAXSStart):
                logger.info(f"Processing Start {message}")
                self.current_scan_metadata = message
                logger.info("Calculating mask")
                reduction_settings = await self.redis_conn.get_json(REDUCTION_CONFIG_KEY)
                # Currently a static file for the mask is loaded. Future iterations it can be generated dynamically
                # self.mask = await asyncio.to_thread(self.calculate_mask, reduction_settings)
                await self.publish(message)

            if isinstance(message, GISAXSStop):
                logger.info(f"Processing Stop {message}")
                self.current_scan_metadata = None
                self.current_reduction_settings = None
                await self.publish(message)

            if isinstance(message, GISAXSRawEvent):
                if self.current_scan_metadata is None:
                    logger.error(
                        "No current scan metadata. Perhaps the Viz Operator was started mid-scan?"
                    )
                    return
                reduction_settings = await self.redis_conn.get_json(REDUCTION_CONFIG_KEY)
                if reduction_settings is None or len(reduction_settings) == 0:
                    logger.error("No reduction settings found")
                    return
                reduction_settings.pop("input_uri_data")
                reduction_settings.pop("input_uri_mask")
                masked_image = self.generate_masked_image(message.image.array, self.mask)
                reduction_settings["masked_image"] = masked_image
                reduction, _, _ = await asyncio.to_thread(
                    pixel_roi_horizontal_cut, **reduction_settings
                )
                #
                serializable_reduction = SerializableNumpyArrayModel(array=reduction)
                reduction_msg = GISAXS1DReduction(
                    curve=serializable_reduction,  # just the qparrallel, not the cut_average or errors
                    curve_tiled_url="curve",
                    raw_frame=message.image,
                    raw_frame_tiled_url=message.tiled_url,
                )
                await self.publish(reduction_msg)
        except Exception as e:
            logger.error(f"Error in process: {e}")

    def calculate_mask(self, reduction_settings: dict):
        beamstop = (
            reduction_settings.get("beamcenter_x"),
            reduction_settings.get("beamcenter_y"),
        )
        mask = VerticalPilatus900kw().calc_mask(beamstop)
        return mask

    async def compute_callback(self, data):
        try:
            if data != "compute_reduction":
                return
            reduction_settings = await self.redis_conn.get_json(REDUCTION_CONFIG_KEY)
            (reduction, line_average, errror) = await asyncio.to_thread(
                self.do_reduction, reduction_settings
            )
            reduction_msg = GISAXS1DReduction(
                curve=reduction[0],
                curve_tiled_url="curve", 
                raw_frame=None,
                raw_frame_tiled_url=self.current_run_url,
            )

            await self.publish(reduction_msg)
        except Exception as e:
            logger.error(f"Error in compute_callback: {e}")

    def do_reduction(self, reduction_settings: dict) -> tuple:
        try:
            if reduction_settings is None:
                logger.error("No reduction settings found")
                return
            reduction_settings.pop("input_uri_data")
            mask_uri = reduction_settings.pop("input_uri_mask")
            image_container = get_nested_client(self.tiled_client, mask_uri)
            image = image_container
            # mask = self.calculate_mask(reduction_settings)
            # masked_image = image[0][0] + mask.T
            #reduction_settings["masked_image"] = masked_image
            #reduction_settings["masked_image"] = image
            reduction = pixel_roi_horizontal_cut(**reduction_settings)
            return reduction
        except Exception as e:
            logger.error(f"Error in reduction: {e}")
    
    def load_static_mask_file(self):
        try:
            # assumed path from project root is masks/mask.npy 
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
            mask_path = os.path.join(project_root, "masks", "mask.npy")
            mask = np.load(mask_path)
            logger.info(f"Mask loaded successfully from {mask_path}, shape: {mask.shape}")
            return mask

        except FileNotFoundError:
            logger.error(f"Mask file not found at: {mask_path}")
        except Exception as e:
            logger.error(f"Error loading mask file: {e}")
        return None
    
    def generate_masked_image(image, mask):
        masked_float = mask.astype(float)  
        masked_float[masked_float == True] = np.nan  #At true values set to NaN
        masked_float[masked_float == 0] = 1 #At false values set to 1
        masked_image = image * masked_float #Multiply to set masked values to NaN
        return masked_image
    
    @classmethod
    def from_settings(cls, settings) -> "OneDReductionOperator":
        redis_conn = RedisConn.from_settings(settings.redis)
        tiled_client = from_uri(
            settings.tiled.raw.uri, api_key=settings.tiled.raw.api_key
        )
        return cls(tiled_client, redis_conn)
