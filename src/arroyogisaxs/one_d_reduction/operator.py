import logging

import numpy as np
import pandas as pd
from arroyopy.operator import Operator
from arroyopy.schemas import DataFrameModel

from ..schemas import GISAXS1DReduction, GISAXSRawEvent, GISAXSRawStart, GISAXSRawStop

logger = logging.getLogger(__name__)


class OneDReductionOperator(Operator):
    def __init__(self):
        super().__init__()
        self.dummy1d = Dummy1D()
        self.current_run_url = None
        self.show_msg = True

    async def process(self, message):
        if isinstance(message, GISAXSRawStart):
            self.current_run_url = message.tiled_url
            self.show_msg = True
            logger.info(f"Processing Start {message}")
            await self.publish(message)

        if isinstance(message, GISAXSRawStop):
            logger.info(f"Processing Stio {message}")
            await self.publish(message)

        if isinstance(message, GISAXSRawEvent):
            curve_df = self.dummy1d.next_curve()
            if self.current_run_url is None:
                if self.show_msg:
                    logger.info(
                        "It looks like this was started out of order. Waiting for start message."
                    )
                    self.show_msg = False
                return
            curve = DataFrameModel(df=curve_df)
            reduction_msg = GISAXS1DReduction(
                curve=curve,
                curve_tiled_url=self.current_run_url,
                raw_frame=message.image,
                raw_frame_tiled_url=self.current_run_url,
            )
            await self.publish(reduction_msg)


class Dummy1D:
    def __init__(self) -> pd.DataFrame:
        self.angle = 0

    def next_curve(self):
        if self.angle == 180:
            self.angle = 0
        wave = np.sin(np.linspace(self.angle, np.pi, 180))
        self.angle += 1
        df = pd.DataFrame([wave])
        return df
