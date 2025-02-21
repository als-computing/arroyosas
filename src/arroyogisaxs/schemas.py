import numpy as np
from arroyopy.schemas import DataFrameModel, Event, Message, Start, Stop
from pydantic import BaseModel, field_serializer, field_validator

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


class SerializableNumpyArrayModel(BaseModel):
    """
    Custom Pydantic model for serializing NumPy arrays.
    """

    array: np.ndarray

    @field_serializer("array")
    def serialize_array(self, value: np.ndarray):
        """Convert NumPy array to a dictionary with bytes and dtype"""
        return {
            "data": value.tobytes(),
            "dtype": str(value.dtype.name),
            "shape": value.shape,
        }

    @field_validator("array", mode="before")
    @classmethod
    def deserialize_array(cls, value):
        """Convert bytes back to NumPy array"""
        if isinstance(value, dict) and "data" in value:
            return np.frombuffer(value["data"], dtype=np.dtype(value["dtype"])).reshape(
                value["shape"]
            )
        return value

    class Config:
        arbitrary_types_allowed = True


class GISAXSMessage(Message):
    pass


class GISAXSStart(Start, GISAXSMessage):
    msg_type: str = "start"
    run_name: str
    run_id: str
    width: int
    height: int
    data_type: str
    tiled_url: str


class GISAXSRawEvent(Event, GISAXSMessage):
    msg_type: str = "event"
    image: SerializableNumpyArrayModel
    frame_number: int
    tiled_url: str


class GISAXSLatentSpaceEvent(Event, GISAXSMessage):
    tiled_url: str
    feature_vector: list[float]
    index: int


class GISAXSStop(Stop, GISAXSMessage):
    msg_type: str = "stop"
    num_frames: int


class GISAXSResultStop(Stop, GISAXSMessage):
    msg_type: str = "result_stop"
    function_timings: DataFrameModel


class GISAXS1DReduction(Event, GISAXSMessage):
    curve: SerializableNumpyArrayModel
    curve_tiled_url: str
    raw_frame: SerializableNumpyArrayModel
    raw_frame_tiled_url: str
