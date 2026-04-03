"""Tests for arroyosas.lse_reduction.operator (LatentSpaceOperator)"""

from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from arroyosas.lse_reduction.operator import LatentSpaceOperator
from arroyosas.lse_reduction.reducer import Reducer
from arroyosas.lse_reduction.schemas import LatentSpaceEvent
from arroyosas.schemas import RawFrameEvent, SASStart, SASStop, SerializableNumpyArrayModel


@pytest.fixture
def mock_reducer():
    reducer = MagicMock(spec=Reducer)
    reducer.reduce.return_value = (np.array([[1.0, 2.0]]), {"autoencoder_time": 0.1, "dimred_time": 0.05})
    reducer.is_loading_model = False
    reducer.loading_model_type = None
    reducer.autoencoder_model_name = "ae_v1:1"
    reducer.dimred_model_name = "umap_v1:1"
    reducer.experiment_name = "test_exp"
    return reducer


@pytest.fixture
def mock_redis_store():
    store = MagicMock()
    store.get_autoencoder_model.return_value = "ae_v1:1"
    store.get_dimred_model.return_value = "umap_v1:1"
    return store


@pytest.fixture
def operator(mock_reducer, mock_redis_store):
    op = LatentSpaceOperator(mock_reducer, mock_redis_store)
    op._publishers = []
    return op


def _make_raw_frame():
    image = SerializableNumpyArrayModel(array=np.zeros((10, 10), dtype=np.float32))
    return RawFrameEvent(image=image, frame_number=1, tiled_url="http://example.com/run/uuid-abc/data")


class TestLatentSpaceOperator:
    def test_init(self, mock_reducer, mock_redis_store):
        op = LatentSpaceOperator(mock_reducer, mock_redis_store)
        assert op.reducer is mock_reducer
        assert op.redis_model_store is mock_redis_store
        assert op._flush_sent is False

    def test_check_models_selected_returns_models(self, operator, mock_redis_store):
        mock_redis_store.get_autoencoder_model.return_value = "ae"
        mock_redis_store.get_dimred_model.return_value = "umap"
        ae, dim = operator._check_models_selected()
        assert ae == "ae"
        assert dim == "umap"

    def test_check_models_selected_no_store(self, mock_reducer):
        op = LatentSpaceOperator(mock_reducer, None)
        ae, dim = op._check_models_selected()
        assert ae is None
        assert dim is None

    def test_check_models_selected_exception(self, operator, mock_redis_store):
        mock_redis_store.get_autoencoder_model.side_effect = Exception("Redis error")
        ae, dim = operator._check_models_selected()
        assert ae is None
        assert dim is None

    async def test_process_start_message(self, operator):
        with patch.object(operator, "publish", new=AsyncMock()) as mock_pub:
            start = SASStart(
                run_name="run1",
                run_id="id1",
                width=10,
                height=10,
                data_type="float32",
                tiled_url="http://example.com",
            )
            await operator.process(start)
            mock_pub.assert_called_once_with(start)

    async def test_process_stop_message(self, operator):
        with patch.object(operator, "publish", new=AsyncMock()) as mock_pub:
            stop = SASStop(num_frames=5)
            await operator.process(stop)
            mock_pub.assert_called_once_with(stop)

    async def test_process_raw_frame_with_models(self, operator, mock_reducer, mock_redis_store):
        mock_redis_store.get_autoencoder_model.return_value = "ae_v1"
        mock_redis_store.get_dimred_model.return_value = "umap_v1"

        with patch.object(operator, "publish", new=AsyncMock()):
            with patch.object(operator, "dispatch", new=AsyncMock(return_value=None)):
                frame = _make_raw_frame()
                await operator.process(frame)

    async def test_process_raw_frame_no_models_skips(self, operator, mock_redis_store):
        mock_redis_store.get_autoencoder_model.return_value = None
        mock_redis_store.get_dimred_model.return_value = None

        with patch.object(operator, "publish", new=AsyncMock()):
            frame = _make_raw_frame()
            await operator.process(frame)
            # Should not publish raw frame

    async def test_process_raw_frame_no_store(self, mock_reducer):
        op = LatentSpaceOperator(mock_reducer, None)
        op._publishers = []
        with (
            patch.object(op, "publish", new=AsyncMock()) as mock_pub,
            patch.object(op, "dispatch", new=AsyncMock(return_value=None)),
        ):
            frame = _make_raw_frame()
            await op.process(frame)
            # With no store, should still publish the frame
            mock_pub.assert_called_with(frame)

    async def test_process_unknown_message_type(self, operator):
        msg = MagicMock()
        msg.__class__ = type("Unknown", (), {})
        with patch.object(operator, "publish", new=AsyncMock()) as mock_pub:
            await operator.process(msg)
            mock_pub.assert_not_called()

    async def test_dispatch_no_store_calls_reducer(self, operator, mock_reducer):
        operator.redis_model_store = None
        mock_reducer.reduce.return_value = (np.array([[1.0, 2.0]]), {"autoencoder_time": 0.1, "dimred_time": 0.05})
        mock_reducer.is_loading_model = False
        mock_reducer.autoencoder_model_name = "ae"
        mock_reducer.dimred_model_name = "umap"
        mock_reducer.experiment_name = "exp"

        with patch.object(operator, "publish", new=AsyncMock()):
            frame = _make_raw_frame()
            result = await operator.dispatch(frame)

        assert isinstance(result, LatentSpaceEvent)
        assert result.feature_vector == [1.0, 2.0]

    async def test_dispatch_model_loading_returns_none(self, operator, mock_reducer, mock_redis_store):
        mock_redis_store.get_autoencoder_model.return_value = "ae"
        mock_redis_store.get_dimred_model.return_value = "umap"
        mock_reducer.is_loading_model = True
        mock_reducer.loading_model_type = "autoencoder"

        with patch.object(operator, "publish", new=AsyncMock()):
            frame = _make_raw_frame()
            result = await operator.dispatch(frame)

        assert result is None

    async def test_dispatch_no_models_sends_flush_once(self, operator, mock_redis_store):
        mock_redis_store.get_autoencoder_model.return_value = None
        mock_redis_store.get_dimred_model.return_value = None

        with patch.object(operator, "publish", new=AsyncMock()) as mock_pub:
            frame = _make_raw_frame()
            result = await operator.dispatch(frame)
            assert result is None
            # First call should send flush
            assert operator._flush_sent is True
            flush_calls = [c for c in mock_pub.call_args_list if isinstance(c[0][0], LatentSpaceEvent)]
            assert len(flush_calls) == 1
            assert flush_calls[0][0][0].tiled_url == "FLUSH_SIGNAL"

    async def test_dispatch_no_models_sends_flush_only_once(self, operator, mock_redis_store):
        mock_redis_store.get_autoencoder_model.return_value = None
        mock_redis_store.get_dimred_model.return_value = None

        with patch.object(operator, "publish", new=AsyncMock()) as mock_pub:
            frame = _make_raw_frame()
            await operator.dispatch(frame)
            await operator.dispatch(frame)
            flush_calls = [c for c in mock_pub.call_args_list if isinstance(c[0][0], LatentSpaceEvent)]
            assert len(flush_calls) == 1  # Only sent once

    async def test_dispatch_reducer_returns_none(self, operator, mock_reducer, mock_redis_store):
        mock_redis_store.get_autoencoder_model.return_value = "ae"
        mock_redis_store.get_dimred_model.return_value = "umap"
        mock_reducer.is_loading_model = False
        mock_reducer.reduce.return_value = (None, {"autoencoder_time": None, "dimred_time": None})

        with patch.object(operator, "publish", new=AsyncMock()):
            frame = _make_raw_frame()
            result = await operator.dispatch(frame)

        assert result is None

    async def test_dispatch_exception_returns_none(self, operator, mock_reducer, mock_redis_store):
        mock_redis_store.get_autoencoder_model.return_value = "ae"
        mock_redis_store.get_dimred_model.return_value = "umap"
        mock_reducer.is_loading_model = False
        mock_reducer.reduce.side_effect = Exception("reduction error")

        with patch.object(operator, "publish", new=AsyncMock()):
            frame = _make_raw_frame()
            result = await operator.dispatch(frame)

        assert result is None
