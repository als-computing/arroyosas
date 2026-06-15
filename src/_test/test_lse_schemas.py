"""Tests for arroyosas.lse_reduction.schemas"""

import numpy as np

from arroyosas.lse_reduction.schemas import LatentSpaceEvent, SerializableNumpyArrayModel


class TestSerializableNumpyArrayModel:
    def test_create_with_array(self):
        arr = np.array([1.0, 2.0, 3.0])
        model = SerializableNumpyArrayModel(array=arr)
        np.testing.assert_array_equal(model.array, arr)

    def test_serialize_array(self):
        arr = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
        model = SerializableNumpyArrayModel(array=arr)
        dumped = model.model_dump()
        serialized = dumped["array"]
        assert "data" in serialized
        assert "dtype" in serialized
        assert "shape" in serialized
        assert serialized["dtype"] == "float32"
        assert serialized["shape"] == (2, 2)

    def test_deserialize_from_dict(self):
        arr = np.array([[1, 2], [3, 4]], dtype=np.int32)
        serialized = {
            "data": arr.tobytes(),
            "dtype": "int32",
            "shape": (2, 2),
        }
        model = SerializableNumpyArrayModel(array=serialized)
        np.testing.assert_array_equal(model.array, arr)

    def test_roundtrip(self):
        arr = np.random.rand(4, 5).astype(np.float64)
        model = SerializableNumpyArrayModel(array=arr)
        dumped = model.model_dump()
        restored = SerializableNumpyArrayModel(array=dumped["array"])
        np.testing.assert_array_almost_equal(restored.array, arr)

    def test_arbitrary_types_allowed(self):
        arr = np.zeros((3, 3), dtype=np.uint8)
        model = SerializableNumpyArrayModel(array=arr)
        assert model.array.dtype == np.uint8


class TestLatentSpaceEvent:
    def test_create_minimal(self):
        event = LatentSpaceEvent(
            tiled_url="http://example.com/tiled",
            feature_vector=[1.0, 2.0, 3.0],
            index=0,
        )
        assert event.tiled_url == "http://example.com/tiled"
        assert event.feature_vector == [1.0, 2.0, 3.0]
        assert event.index == 0

    def test_optional_fields_default_to_none(self):
        event = LatentSpaceEvent(
            tiled_url="http://example.com",
            feature_vector=[0.1],
            index=5,
        )
        assert event.autoencoder_model is None
        assert event.dimred_model is None
        assert event.experiment_name is None
        assert event.timestamp is None
        assert event.total_processing_time is None
        assert event.autoencoder_time is None
        assert event.dimred_time is None

    def test_create_with_all_fields(self):
        event = LatentSpaceEvent(
            tiled_url="http://example.com",
            feature_vector=[0.1, 0.2],
            index=42,
            autoencoder_model="ae_v1",
            dimred_model="umap_v1",
            experiment_name="my_experiment",
            timestamp=1234567890.0,
            total_processing_time=0.5,
            autoencoder_time=0.3,
            dimred_time=0.2,
        )
        assert event.autoencoder_model == "ae_v1"
        assert event.dimred_model == "umap_v1"
        assert event.experiment_name == "my_experiment"
        assert event.timestamp == 1234567890.0
        assert event.total_processing_time == 0.5

    def test_model_dump_json(self):
        event = LatentSpaceEvent(
            tiled_url="http://example.com",
            feature_vector=[1.0, 2.0],
            index=0,
        )
        json_str = event.model_dump_json()
        assert "tiled_url" in json_str
        assert "feature_vector" in json_str
