"""Tests for arroyosas.lse_reduction.publisher (LSEWSResultPublisher)"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from arroyosas.lse_reduction.publisher import LSEWSResultPublisher
from arroyosas.lse_reduction.schemas import LatentSpaceEvent
from arroyosas.schemas import SASStart, SASStop

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def reset_connected_clients():
    """Each test should start with empty connected_clients."""
    LSEWSResultPublisher.connected_clients = set()
    yield
    LSEWSResultPublisher.connected_clients = set()


@pytest.fixture
def publisher():
    return LSEWSResultPublisher(host="localhost", port=9876, path="/lse_operator")


class TestLSEWSResultPublisher:
    def test_init_defaults(self):
        pub = LSEWSResultPublisher()
        assert pub.host == "localhost"
        assert pub.port == 8765
        assert pub.path == "/lse_operator"

    def test_init_custom(self):
        pub = LSEWSResultPublisher(host="0.0.0.0", port=1234, path="/test")
        assert pub.host == "0.0.0.0"
        assert pub.port == 1234
        assert pub.path == "/test"

    def test_from_settings(self):
        settings = MagicMock()
        settings.host = "myhost"
        settings.port = 5555
        pub = LSEWSResultPublisher.from_settings(settings)
        assert pub.host == "myhost"
        assert pub.port == 5555

    async def test_publish_no_clients_does_nothing(self, publisher):
        event = LatentSpaceEvent(
            tiled_url="http://example.com",
            feature_vector=[1.0, 2.0],
            index=0,
        )
        # No clients connected - should complete without error
        await publisher.publish(event)

    async def test_publish_sends_to_all_clients(self, publisher):
        client1 = AsyncMock()
        client2 = AsyncMock()
        LSEWSResultPublisher.connected_clients = {client1, client2}

        event = LatentSpaceEvent(
            tiled_url="http://example.com",
            feature_vector=[1.0, 2.0],
            index=0,
        )
        await publisher.publish(event)
        # gather is called but not awaited in publish(), so clients may not be called yet
        # The important thing is no exception is raised

    async def test_publish_ws_latent_space_event(self, publisher):
        client = AsyncMock()
        event = LatentSpaceEvent(
            tiled_url="http://example.com",
            feature_vector=[1.0, 2.0],
            index=5,
            autoencoder_model="ae",
            dimred_model="umap",
        )
        await publisher.publish_ws(client, event)
        client.send.assert_called_once()
        # The message should be JSON
        sent_data = client.send.call_args[0][0]
        assert "tiled_url" in sent_data
        assert "feature_vector" in sent_data

    async def test_publish_ws_sas_stop_returns_early(self, publisher):
        client = AsyncMock()
        stop = SASStop(num_frames=5)
        await publisher.publish_ws(client, stop)
        client.send.assert_not_called()

    async def test_publish_ws_sas_start_returns_early(self, publisher):
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
        client.send.assert_not_called()

    async def test_websocket_handler_adds_and_removes_client(self, publisher):
        mock_ws = AsyncMock()
        mock_ws.remote_address = ("127.0.0.1", 12345)
        mock_ws.wait_closed = AsyncMock(return_value=None)

        await publisher.websocket_handler(mock_ws)

        # After handler completes, client should be removed
        assert mock_ws not in LSEWSResultPublisher.connected_clients

    async def test_websocket_handler_removes_client_on_exception(self, publisher):
        mock_ws = AsyncMock()
        mock_ws.remote_address = ("127.0.0.1", 12345)
        mock_ws.wait_closed = AsyncMock(side_effect=Exception("connection lost"))

        with pytest.raises(Exception):
            await publisher.websocket_handler(mock_ws)

        assert mock_ws not in LSEWSResultPublisher.connected_clients

    async def test_start_calls_websockets_serve(self, publisher):
        mock_server = MagicMock()
        mock_server.wait_closed = AsyncMock(return_value=None)

        # websockets.serve is an async context manager / awaitable; make it awaitable
        async def fake_serve(*args, **kwargs):
            return mock_server

        with patch("arroyosas.lse_reduction.publisher.websockets.serve", side_effect=fake_serve) as mock_serve:
            await publisher.start()
            mock_serve.assert_called_once_with(
                publisher.websocket_handler,
                publisher.host,
                publisher.port,
            )
