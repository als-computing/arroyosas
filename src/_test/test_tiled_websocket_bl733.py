"""Tests for arroyosas.tiled.tiled_websocket_bl733 (TiledClientListener)"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from arroyosas.schemas import RawFrameEvent, SASStart
from arroyosas.tiled.tiled_websocket_bl733 import TiledClientListener, create_tiled_websocket_listener


@pytest.fixture
def mock_tiled_client():
    client = MagicMock()
    client.context = MagicMock()
    client.context.base_url = "http://example.com"
    return client


@pytest.fixture
def mock_operator():
    op = AsyncMock()
    op.notify = AsyncMock()
    return op


@pytest.fixture
def listener(mock_operator, mock_tiled_client):
    return TiledClientListener(
        operator=mock_operator,
        tiled_client=mock_tiled_client,
        sub_path="runs",
    )


class TestTiledClientListenerBl733:
    def test_init(self, mock_operator, mock_tiled_client):
        listener = TiledClientListener(mock_operator, mock_tiled_client, "runs")
        assert listener.tiled_client is mock_tiled_client
        assert listener.sub_path == "runs"

    async def test_stop(self, listener):
        listener._running = True
        listener._stop_event = asyncio.Event()
        await listener.stop()
        assert listener._running is False
        assert listener._stop_event.is_set()

    def test_on_new_data_collection_no_key(self, listener):
        sub = MagicMock()
        data = {}  # No 'key'
        listener.on_new_data_collection(sub, data)
        # Should return early without creating subscription

    def test_on_new_data_collection_with_key(self, listener, mock_tiled_client):
        sub = MagicMock()
        sub.segments = ["runs"]
        data = {"key": "run_001"}

        mock_run_node = MagicMock()
        mock_run_node.metadata = {
            "run_name": "scan_1",
            "width": 100,
            "height": 100,
            "data_type": "float32",
        }
        mock_tiled_client.__getitem__ = MagicMock(return_value=mock_run_node)

        with patch("arroyosas.tiled.tiled_websocket_bl733.Subscription") as mock_sub_cls:
            mock_sub = MagicMock()
            mock_sub_cls.return_value = mock_sub
            listener._loop = asyncio.new_event_loop()

            with patch.object(listener, "send_to_operator"):
                listener.on_new_data_collection(sub, data)

            mock_sub_cls.assert_called()
            mock_sub.add_callback.assert_called()
            mock_sub.start.assert_called()

    def test_on_new_data_item_no_key(self, listener):
        sub = MagicMock()
        data = {}
        listener.on_new_data_item(sub, data)
        # Should return early

    def test_on_new_data_item_with_key(self, listener, mock_tiled_client):
        sub = MagicMock()
        sub.segments = ["runs", "run_001"]
        data = {"key": "frame_001", "sequence": 5}

        # Mock tiled client for data node access
        data_node = MagicMock()
        data_node.__getitem__ = MagicMock(return_value=np.zeros((10, 10), dtype=np.float32))
        mock_tiled_client.__getitem__ = MagicMock(return_value=data_node)

        with patch.object(listener, "send_to_operator") as mock_send:
            listener.on_new_data_item(sub, data)
            mock_send.assert_called_once()
            args = mock_send.call_args[0][0]
            assert isinstance(args, RawFrameEvent)

    def test_publish_start(self, listener, mock_tiled_client):
        sub = MagicMock()
        sub.segments = ["runs"]
        data = {"key": "run_001"}

        mock_run_node = MagicMock()
        mock_run_node.metadata = {
            "run_name": "test_scan",
            "width": 200,
            "height": 150,
            "data_type": "float32",
        }
        mock_tiled_client.__getitem__ = MagicMock(return_value=mock_run_node)

        with patch.object(listener, "send_to_operator") as mock_send:
            listener.publish_start(sub, data)
            mock_send.assert_called_once()
            msg = mock_send.call_args[0][0]
            assert isinstance(msg, SASStart)
            assert msg.run_id == "run_001"
            assert msg.run_name == "test_scan"

    def test_publish_event(self, listener, mock_tiled_client):
        sub = MagicMock()
        sub.segments = ["runs", "run_001"]
        data = {"key": "frame_001", "sequence": 3}

        data_node = MagicMock()
        data_node.__getitem__ = MagicMock(return_value=np.zeros((5, 5)))
        mock_tiled_client.__getitem__ = MagicMock(return_value=data_node)

        with patch.object(listener, "send_to_operator") as mock_send:
            listener.publish_event(sub, data)
            mock_send.assert_called_once()
            msg = mock_send.call_args[0][0]
            assert isinstance(msg, RawFrameEvent)
            assert msg.frame_number == 3

    def test_send_to_operator_calls_notify(self, listener, mock_operator):
        loop = asyncio.new_event_loop()
        listener._loop = loop
        msg = MagicMock()
        with patch("arroyosas.tiled.tiled_websocket_bl733.asyncio.run_coroutine_threadsafe") as mock_future:
            mock_f = MagicMock()
            mock_future.return_value = mock_f
            listener.send_to_operator(msg)
            mock_future.assert_called_once()
        loop.close()

    def test_start_calls_subscriptions(self, listener, mock_tiled_client):
        mock_node = MagicMock()
        mock_node.context = MagicMock()
        mock_node.path_parts = ["runs"]
        mock_tiled_client.__getitem__ = MagicMock(return_value=mock_node)

        with patch("arroyosas.tiled.tiled_websocket_bl733.Subscription") as mock_sub_cls:
            mock_sub = MagicMock()
            mock_sub_cls.return_value = mock_sub
            listener._start()

            mock_sub.add_callback.assert_called_once_with(listener.on_new_data_collection)
            mock_sub.start.assert_called_once()


class TestCreateTiledWebsocketListenerBl733:
    def test_create(self):
        with patch("arroyosas.tiled.tiled_websocket_bl733.from_uri") as mock_from_uri:
            mock_client = MagicMock()
            mock_from_uri.return_value = mock_client
            listener = create_tiled_websocket_listener(
                uri="http://example.com",
                sub_path="runs",
                operator=MagicMock(),
                api_key="test_key",
            )
            assert isinstance(listener, TiledClientListener)
            mock_from_uri.assert_called_once_with("http://example.com", api_key="test_key")
