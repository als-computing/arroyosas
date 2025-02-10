import numpy as np
from arroyopy.operator import Operator

from ..schemas import GISAXS1DReduction, GISAXSRawEvent, GISAXSRawStart, GISAXSRawStop


class OneDReductionOperator(Operator):
    def __init__(self):
        super().__init__()
        self.dummy1d = Dummy1D()
        self.current_run_url = None

    async def process(self, message):
        if isinstance(message, GISAXSRawStart):
            self.dummy1d.clear()
            self.current_run_url = message.tiled_url
            self.publish(message)

        if isinstance(message, GISAXSRawStop):
            self.publish(message)

        if isinstance(message, GISAXSRawEvent):
            curve = self.dummy1d.next_curve()
            reduction_msg = GISAXS1DReduction(
                curve=curve,
                curve_tiled_url=self.current_run_url,
                raw_frame=message.image,
                raw_frame_tiled_url=self.current_run_url,
            )
            self.publish(reduction_msg)


class Dummy1D:
    def __init__(self):
        self.angle = 0

    def next_curve(self):
        if self.angle == 180:
            self.angle = 0
        wave = np.sin(np.linspace(self.angle, np.pi, 180))
        self.angle += 1
        return wave
