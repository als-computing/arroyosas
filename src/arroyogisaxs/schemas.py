from typing import Literal

from arroyopy.schemas import (
    DataFrameModel,
    Event,
    Message,
    NumpyArrayModel,
    Start,
    Stop,
)
from pydantic import BaseModel

"""
    This module defines schemas for GISAXS messages and events using
    Pydantic models. It includes classes for different types of messages and events such as
    start, stop, raw events, and results. These classes serve as data transfer classes within the
    tr_ap_GISAXS pipeline.

    Pydantic is used for several reasons.
    - It provides validated of messages
    - Using pydantic's alias mechanism, it provides a mapping between the json field names produced by LabVIEW and
        python field name.
    - Pydantic provides easy translation between json and python structures

    Three of these models define the incoming message from LabView, one defines the outgoing message
    from our Operators.

"""


class GISAXSMessage(Message):
    pass


class GISAXSStart(Start, GISAXSMessage):
    msg_type: str = Literal["start"]


class GISAXSImageInfo(BaseModel):
    frame_number: int
    width: int
    height: int
    data_type: str


class GISAXSEvent(Event, GISAXSMessage):
    """

    LabVIEW Message:
    {
        "msg_type": "event",
        "Frame Number": 1
    }
    """

    msg_type: str = Literal["event"]
    image: NumpyArrayModel
    image_info: GISAXSImageInfo
    one_d_reduction: DataFrameModel


class GISAXSStop(Stop, GISAXSMessage):
    """
    {
        "msg_type": "stop",
        "Num Frames": 1
    }

    """

    pass
    # num_frames: int = Field(..., alias="Num Frames")


class GISAXSResultStop(Stop, GISAXSMessage):
    msg_type: str = Literal["result_stop"]
    function_timings: DataFrameModel
