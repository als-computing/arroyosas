"""Tests for arroyosas.directWebsocketSim (OneDWSPublisher, convert_to_uint8, pack_images)"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from arroyosas.directWebsocketSim import OneDWSPublisher, convert_to_uint8, pack_images
from arroyosas.schemas import RawFrameEvent, SASStart, SASStop, SerializableNumpyArrayModel

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def reset_connected_clients():
    OneDWSPublisher.connected_clients = set()
    yield
    OneDWSPublisher.connected_clients = set()


@pytest.fixture
def publisher():
    return OneDWSPublisher(host="localhost", port=8001)


# ---------------------------------------------------------------------------
# Pure function tests (directWebsocketSim versions)
# ---------------------------------------------------------------------------


class TestConvertToUint8Direct:
    def test_converts_float_array(self):
        image = np.array([[0.0, 1.0], [0.5, 0.25]], dtype=np.float32)
        result = convert_to_uint8(image)
        assert isinstance(result, bytes)

    def test_output_is_valid_uint8(self):
        image = np.random.rand(8, 8).astype(np.float32)
        result = convert_to_uint8(image)
        arr = np.frombuffer(result, dtype=np.uint8)
        assert arr.min() >= 0
        assert arr.max() <= 255


class TestPackImagesDirect:
    def test_packs_raw_frame_event(self):
        import msgpack

        image = SerializableNumpyArrayModel(array=np.random.rand(5, 5).astype(np.float32))
        event = RawFrameEvent(
            image=image,
            frame_number=0,
            tiled_url="http://example.com/frame",
        )
        result = pack_images(event)
        assert isinstance(result, bytes)
        unpacked = msgpack.unpackb(result, raw=False)
        assert "raw_frame" in unpacked
        assert "raw_frame_tiled_url" in unpacked
        assert unpacked["raw_frame_tiled_url"] == "http://example.com/frame"

    def test_raises_on_invalid_message(self):
        with pytest.raises(Exception):
            pack_images(None)


# ---------------------------------------------------------------------------
# OneDWSPublisher tests (directWebsocketSim version)
# ---------------------------------------------------------------------------


class TestOneDWSPublisherDirect:
    def test_init(self):
        pub = OneDWSPublisher(host="0.0.0.0", port=9001)
        assert pub.host == "0.0.0.0"
        assert pub.port == 9001

    def test_from_settings(self):
        settings = MagicMock()
        settings.host = "myhost"
        settings.port = 5000
        pub = OneDWSPublisher.from_settings(settings)
        assert pub.host == "myhost"
        assert pub.port == 5000

    async def test_publish_no_clients(self, publisher):
        image = SerializableNumpyArrayModel(array=np.zeros((5, 5), dtype=np.float32))
        event = RawFrameEvent(image=image, frame_number=0, tiled_url="http://example.com")
        # No exception
        await publisher.publish(event)

    async def test_publish_ws_stop(self, publisher):
        client = AsyncMock()
        stop = SASStop(num_frames=3)
        await publisher.publish_ws(client, stop)
        client.send.assert_called_once()
        sent = json.loads(client.send.call_args[0][0])
        assert sent["msg_type"] == "stop"
        assert publisher.current_start_message is None

    async def test_publish_ws_start(self, publisher):
        client = AsyncMock()
        start = SASStart(
            run_name="run1",
            run_id="id1",
            width=10,
            height=10,
            data_type="float32",
            tiled_url="http://example.com",
        )
        await publisher.publish_ws(client, start)
        client.send.assert_called_once()
        sent = json.loads(client.send.call_args[0][0])
        assert sent["msg_type"] == "start"
        assert publisher.current_start_message is start

    async def test_publish_ws_raw_frame(self, publisher):
        client = AsyncMock()
        image = SerializableNumpyArrayModel(array=np.random.rand(5, 5).astype(np.float32))
        event = RawFrameEvent(image=image, frame_number=0, tiled_url="http://example.com")
        await publisher.publish_ws(client, event)
        client.send.assert_called_once()
        # Sent bytes (msgpack)
        assert isinstance(client.send.call_args[0][0], bytes)

    async def test_websocket_handler_wrong_path(self, publisher):
        mock_ws = AsyncMock()
        mock_ws.remote_address = ("127.0.0.1", 9999)
        mock_ws.request = MagicMock()
        mock_ws.request.path = "/wrong"

        await publisher.websocket_handler(mock_ws)
        assert mock_ws not in OneDWSPublisher.connected_clients

    async def test_websocket_handler_correct_path(self, publisher):
        mock_ws = AsyncMock()
        mock_ws.remote_address = ("127.0.0.1", 9999)
        mock_ws.request = MagicMock()
        mock_ws.request.path = "/viz"
        mock_ws.wait_closed = AsyncMock(return_value=None)

        await publisher.websocket_handler(mock_ws)
        # Removed after handler completes
        assert mock_ws not in OneDWSPublisher.connected_clients

    async def test_start_serves_websocket(self, publisher):
        mock_server = MagicMock()
        mock_server.wait_closed = AsyncMock(return_value=None)

        async def fake_serve(*args, **kwargs):
            return mock_server

        with patch("arroyosas.directWebsocketSim.websockets.serve", side_effect=fake_serve) as mock_serve:
            await publisher.start()
            mock_serve.assert_called_once_with(publisher.websocket_handler, publisher.host, publisher.port)
