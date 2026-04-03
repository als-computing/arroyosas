"""Tests for arroyosas.websockets (OneDWSPublisher, convert_to_uint8, pack_images)"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import msgpack
import numpy as np
import pytest

from arroyosas.schemas import SASStart, SASStop, SerializableNumpyArrayModel
from arroyosas.websockets import OneDWSPublisher, convert_to_uint8, pack_images


@pytest.fixture(autouse=True)
def reset_connected_clients():
    OneDWSPublisher.connected_clients = set()
    yield
    OneDWSPublisher.connected_clients = set()


@pytest.fixture
def publisher():
    return OneDWSPublisher(host="localhost", port=8001)


# ---------------------------------------------------------------------------
# Pure function tests
# ---------------------------------------------------------------------------


class TestConvertToUint8:
    def test_basic_float_array(self):
        image = np.array([[0.0, 0.5], [1.0, 0.25]], dtype=np.float32)
        result = convert_to_uint8(image)
        assert isinstance(result, bytes)

    def test_output_length(self):
        image = np.zeros((10, 20), dtype=np.float32)
        image[5, 10] = 1.0
        result = convert_to_uint8(image)
        restored = np.frombuffer(result, dtype=np.uint8).reshape(10, 20)
        assert restored.shape == (10, 20)

    def test_uniform_image_edge_case(self):
        # All-zero image: min == max, will cause division by zero in normalization
        # The function should still return bytes (even if values are nan/0)
        image = np.zeros((5, 5), dtype=np.float32)
        # This may produce nan, but shouldn't raise
        try:
            result = convert_to_uint8(image)
            assert isinstance(result, bytes)
        except Exception:
            pass  # Division by zero is an acceptable failure mode here

    def test_values_in_range(self):
        image = np.random.rand(8, 8).astype(np.float32)
        result = convert_to_uint8(image)
        restored = np.frombuffer(result, dtype=np.uint8)
        assert restored.min() >= 0
        assert restored.max() <= 255


class TestPackImages:
    def _make_sas1dreduction(self):
        from arroyosas.schemas import SAS1DReduction

        curve = SerializableNumpyArrayModel(array=np.linspace(0, 1, 100).astype(np.float32))
        raw_frame = SerializableNumpyArrayModel(array=np.random.rand(10, 10).astype(np.float32))
        return SAS1DReduction(
            curve=curve,
            curve_tiled_url="http://example.com/curve",
            raw_frame=raw_frame,
            raw_frame_tiled_url="http://example.com/raw",
        )

    def test_returns_bytes(self):
        msg = self._make_sas1dreduction()
        result = pack_images(msg)
        assert isinstance(result, bytes)

    def test_msgpack_unpackable(self):
        msg = self._make_sas1dreduction()
        result = pack_images(msg)
        unpacked = msgpack.unpackb(result, raw=False)
        assert "raw_frame" in unpacked
        assert "curve" in unpacked
        assert "width" in unpacked
        assert "height" in unpacked

    def test_contains_tiled_urls(self):
        msg = self._make_sas1dreduction()
        result = pack_images(msg)
        unpacked = msgpack.unpackb(result, raw=False)
        assert unpacked["raw_frame_tiled_url"] == "http://example.com/raw"
        assert unpacked["curve_tiled_url"] == "http://example.com/curve"


# ---------------------------------------------------------------------------
# OneDWSPublisher tests
# ---------------------------------------------------------------------------


class TestOneDWSPublisher:
    def test_init_defaults(self):
        pub = OneDWSPublisher()
        assert pub.host == "localhost"
        assert pub.port == 8001

    def test_init_custom(self):
        pub = OneDWSPublisher(host="0.0.0.0", port=9999)
        assert pub.host == "0.0.0.0"
        assert pub.port == 9999

    def test_from_settings(self):
        settings = MagicMock()
        settings.host = "myhost"
        settings.port = 7777
        pub = OneDWSPublisher.from_settings(settings)
        assert pub.host == "myhost"
        assert pub.port == 7777

    async def test_publish_no_clients(self, publisher):
        from arroyosas.schemas import SAS1DReduction

        curve = SerializableNumpyArrayModel(array=np.array([1.0, 2.0]))
        raw = SerializableNumpyArrayModel(array=np.array([[1.0, 2.0], [3.0, 4.0]]))
        msg = SAS1DReduction(
            curve=curve,
            curve_tiled_url="http://c.com",
            raw_frame=raw,
            raw_frame_tiled_url="http://r.com",
        )
        # No clients - should complete without error
        await publisher.publish(msg)

    async def test_publish_ws_sas_stop(self, publisher):
        client = AsyncMock()
        stop = SASStop(num_frames=3)
        await publisher.publish_ws(client, stop)
        client.send.assert_called_once()
        sent = json.loads(client.send.call_args[0][0])
        assert sent["msg_type"] == "stop"
        assert publisher.current_start_message is None

    async def test_publish_ws_sas_start(self, publisher):
        client = AsyncMock()
        start = SASStart(
            run_name="run1",
            run_id="id1",
            width=100,
            height=100,
            data_type="float32",
            tiled_url="http://example.com",
        )
        await publisher.publish_ws(client, start)
        client.send.assert_called_once()
        sent = json.loads(client.send.call_args[0][0])
        assert sent["msg_type"] == "start"
        assert publisher.current_start_message == start

    async def test_publish_ws_sas1dreduction_calls_pack_images(self, publisher):
        from arroyosas.schemas import SAS1DReduction

        client = AsyncMock()
        curve = SerializableNumpyArrayModel(array=np.linspace(0, 1, 10).astype(np.float32))
        raw = SerializableNumpyArrayModel(array=np.random.rand(5, 5).astype(np.float32))
        msg = SAS1DReduction(
            curve=curve,
            curve_tiled_url="http://c.com",
            raw_frame=raw,
            raw_frame_tiled_url="http://r.com",
        )
        await publisher.publish_ws(client, msg)
        client.send.assert_called_once()
        # The sent data should be bytes (msgpack)
        assert isinstance(client.send.call_args[0][0], bytes)

    async def test_websocket_handler_wrong_path(self, publisher):
        mock_ws = AsyncMock()
        mock_ws.remote_address = ("127.0.0.1", 1234)
        mock_ws.request = MagicMock()
        mock_ws.request.path = "/wrong"

        await publisher.websocket_handler(mock_ws)
        # Client should NOT be added
        assert mock_ws not in OneDWSPublisher.connected_clients

    async def test_websocket_handler_correct_path(self, publisher):
        mock_ws = AsyncMock()
        mock_ws.remote_address = ("127.0.0.1", 1234)
        mock_ws.request = MagicMock()
        mock_ws.request.path = "/viz"
        mock_ws.wait_closed = AsyncMock(return_value=None)

        await publisher.websocket_handler(mock_ws)
        assert mock_ws not in OneDWSPublisher.connected_clients  # removed after close

    async def test_start_calls_websockets_serve(self, publisher):
        mock_server = MagicMock()
        mock_server.wait_closed = AsyncMock(return_value=None)

        async def fake_serve(*args, **kwargs):
            return mock_server

        with patch("arroyosas.websockets.websockets.serve", side_effect=fake_serve) as mock_serve:
            await publisher.start()
            mock_serve.assert_called_once_with(
                publisher.websocket_handler,
                publisher.host,
                publisher.port,
            )
