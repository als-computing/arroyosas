import numpy

from arroyogisaxs.schemas import SerializableNumpyArrayModel


def test_serializable_np():
    arr = numpy.zeros((10, 10), dtype=numpy.float32)

    serializable = SerializableNumpyArrayModel(array=arr)
    assert serializable.model_dump() == {
        "array": {"data": arr.tobytes(), "dtype": "float32", "shape": (10, 10)}
    }
    test_arr = SerializableNumpyArrayModel.deserialize_array(
        {"data": arr.tobytes(), "dtype": arr.dtype.name, "shape": arr.shape}
    )
    assert numpy.array_equal(test_arr, serializable.array)
