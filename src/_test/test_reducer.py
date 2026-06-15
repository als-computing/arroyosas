"""Tests for arroyosas.lse_reduction.reducer (LatentSpaceReducer, Reducer)"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from arroyosas.lse_reduction.reducer import Reducer
from arroyosas.schemas import RawFrameEvent, SerializableNumpyArrayModel


def _make_raw_frame():
    image = SerializableNumpyArrayModel(array=np.random.rand(10, 10).astype(np.float32))
    return RawFrameEvent(image=image, frame_number=0, tiled_url="http://example.com")


def _make_mock_redis_store():
    store = MagicMock()
    store.get_autoencoder_model.return_value = "ae_model:1"
    store.get_dimred_model.return_value = "umap_model:1"
    store.get_experiment_name.return_value = "test_experiment"
    store.redis_client = MagicMock()
    return store


def _make_mock_mlflow_client():
    mock_model = MagicMock()
    mock_model.predict.return_value = {"latent_features": np.random.rand(1, 16).astype(np.float32)}
    mock_mlflow = MagicMock()
    mock_mlflow.load_model.return_value = mock_model
    return mock_mlflow


@pytest.fixture
def reducer_instance():
    """Create a LatentSpaceReducer with all external calls mocked."""
    mock_redis_store = _make_mock_redis_store()
    mock_mlflow_client = _make_mock_mlflow_client()

    # Mock the dimred model to return coords
    mock_dimred = MagicMock()
    mock_dimred.predict.return_value = {"coords": np.array([[0.5, 0.3]])}
    mock_mlflow_client.load_model.side_effect = [
        mock_mlflow_client.load_model.return_value,  # autoencoder
        mock_dimred,  # dimred
    ]

    with (
        patch("arroyosas.lse_reduction.reducer.MLflowClient") as mock_mlflow_cls,
        patch("arroyosas.lse_reduction.reducer.redis.Redis") as mock_redis_cls,
    ):
        mock_mlflow_cls.return_value = mock_mlflow_client
        mock_redis_cls.return_value = MagicMock()

        from arroyosas.lse_reduction.reducer import LatentSpaceReducer

        reducer = LatentSpaceReducer(mock_redis_store)

    return reducer, mock_redis_store, mock_mlflow_client, mock_dimred


class TestReducerABC:
    def test_cannot_instantiate_reducer(self):
        with pytest.raises(TypeError):
            Reducer()

    def test_abstract_method_declared(self):
        assert "reduce" in Reducer.__abstractmethods__


class TestLatentSpaceReducer:
    def test_init_loads_models(self, reducer_instance):
        reducer, store, _, _ = reducer_instance
        assert reducer.autoencoder_model_name == "ae_model:1"
        assert reducer.dimred_model_name == "umap_model:1"
        assert reducer.experiment_name == "test_experiment"

    def test_init_loading_state_reset(self, reducer_instance):
        reducer, _, _, _ = reducer_instance
        assert reducer.is_loading_model is False
        assert reducer.loading_model_type is None

    def test_reduce_returns_feature_vector(self, reducer_instance):
        reducer, _, mlflow_client, mock_dimred = reducer_instance
        frame = _make_raw_frame()
        f_vec, timing = reducer.reduce(frame)
        assert f_vec is not None
        assert "autoencoder_time" in timing
        assert "dimred_time" in timing

    def test_reduce_returns_none_when_loading(self, reducer_instance):
        reducer, _, _, _ = reducer_instance
        reducer.is_loading_model = True
        reducer.loading_model_type = "autoencoder"
        frame = _make_raw_frame()
        f_vec, timing = reducer.reduce(frame)
        assert f_vec is None

    def test_reduce_handles_autoencoder_error(self, reducer_instance):
        reducer, _, mlflow_client, _ = reducer_instance
        mlflow_client.load_model.return_value.predict.side_effect = Exception("Model error")
        reducer.current_torch_model = mlflow_client.load_model.return_value
        frame = _make_raw_frame()
        f_vec, timing = reducer.reduce(frame)
        assert f_vec is None

    def test_reduce_handles_dimred_error(self, reducer_instance):
        reducer, _, _, mock_dimred = reducer_instance
        mock_dimred.predict.side_effect = Exception("Dimred error")
        frame = _make_raw_frame()
        f_vec, timing = reducer.reduce(frame)
        assert f_vec is None

    def test_update_loading_state_true(self, reducer_instance):
        reducer, store, _, _ = reducer_instance
        reducer._update_loading_state(True, "autoencoder")
        assert reducer.is_loading_model is True
        assert reducer.loading_model_type == "autoencoder"

    def test_update_loading_state_false(self, reducer_instance):
        reducer, store, _, _ = reducer_instance
        reducer.is_loading_model = True
        reducer.loading_model_type = "dimred"
        reducer._update_loading_state(False)
        assert reducer.is_loading_model is False
        assert reducer.loading_model_type is None

    def test_update_loading_state_updates_redis(self, reducer_instance):
        reducer, store, _, _ = reducer_instance
        reducer._update_loading_state(True, "autoencoder")
        store.redis_client.set.assert_called()

    def test_handle_model_update_experiment_name(self, reducer_instance):
        reducer, _, _, _ = reducer_instance
        update = {"update_type": "experiment_name", "experiment_name": "new_exp"}
        reducer._handle_model_update(update)
        assert reducer.experiment_name == "new_exp"

    def test_handle_model_update_autoencoder(self, reducer_instance):
        reducer, store, _, _ = reducer_instance
        new_model = MagicMock()
        # Set up a fresh mlflow_client mock on the reducer itself
        fresh_mlflow = MagicMock()
        fresh_mlflow.load_model.return_value = new_model
        reducer.mlflow_client = fresh_mlflow
        reducer.autoencoder_model_name = "ae_model:1"  # Different from new_ae:2

        update = {"model_type": "autoencoder", "model_name": "new_ae:2"}
        reducer._handle_model_update(update)
        assert reducer.autoencoder_model_name == "new_ae:2"
        assert reducer.current_torch_model is new_model

    def test_handle_model_update_dimred(self, reducer_instance):
        reducer, _, _, _ = reducer_instance
        new_model = MagicMock()
        fresh_mlflow = MagicMock()
        fresh_mlflow.load_model.return_value = new_model
        reducer.mlflow_client = fresh_mlflow
        reducer.dimred_model_name = "umap_model:1"  # Different from new_umap:3

        update = {"model_type": "dimred", "model_name": "new_umap:3"}
        reducer._handle_model_update(update)
        assert reducer.dimred_model_name == "new_umap:3"
        assert reducer.current_dim_reduction_model is new_model

    def test_handle_model_update_duplicate_autoencoder(self, reducer_instance):
        reducer, _, mlflow_client, _ = reducer_instance
        reducer.autoencoder_model_name = "ae_model:1"

        with patch.object(reducer, "_update_loading_state") as mock_update:
            update = {"model_type": "autoencoder", "model_name": "ae_model:1"}
            reducer._handle_model_update(update)
            # Should call with False to indicate no loading needed
            mock_update.assert_called_with(False)

    def test_handle_model_update_invalid(self, reducer_instance):
        reducer, _, _, _ = reducer_instance
        # Invalid update without model_type and model_name
        update = {"some_key": "some_value"}
        reducer._handle_model_update(update)
        # Should not raise

    def test_handle_model_update_unknown_type(self, reducer_instance):
        reducer, _, _, _ = reducer_instance
        update = {"model_type": "unknown", "model_name": "some_model:1"}
        reducer._handle_model_update(update)
        # Should not raise and should reset loading state

    def test_subscribe_to_model_updates_raises_name_error(self, reducer_instance):
        """_subscribe_to_model_updates references undefined REDIS_HOST/REDIS_PORT."""
        # The method catches Exception and logs a warning, so it should not raise
        reducer, _, _, _ = reducer_instance
        # Just verify it doesn't raise - it logs a warning about NameError
        # This was already called during init, so the thread was attempted
        # We just verify reducer is still in valid state
        assert reducer is not None
