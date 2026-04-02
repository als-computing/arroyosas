"""Tests for arroyosas.app.frame_listener_sim_cli"""

from unittest.mock import AsyncMock, patch

import pytest
from arroyosas.app.frame_listener_sim_cli import process_images

pytestmark = pytest.mark.asyncio


@pytest.fixture
def mock_zmq_socket():
    socket = AsyncMock()
    return socket


class TestProcessImages:
    async def test_process_images_sends_start_stop(self, mock_zmq_socket):
        await process_images(mock_zmq_socket, cycles=1, frames=2, pause=0)
        # Should have sent: start, 2 events, stop = 4 messages
        assert mock_zmq_socket.send.call_count == 4

    async def test_process_images_zero_cycles(self, mock_zmq_socket):
        await process_images(mock_zmq_socket, cycles=0, frames=5, pause=0)
        assert mock_zmq_socket.send.call_count == 0

    async def test_process_images_zero_frames(self, mock_zmq_socket):
        await process_images(mock_zmq_socket, cycles=1, frames=0, pause=0)
        # Just start and stop
        assert mock_zmq_socket.send.call_count == 2

    async def test_process_images_multiple_cycles(self, mock_zmq_socket):
        await process_images(mock_zmq_socket, cycles=3, frames=2, pause=0)
        # 3 cycles * (1 start + 2 frames + 1 stop) = 12
        assert mock_zmq_socket.send.call_count == 12

    async def test_process_images_first_message_is_start(self, mock_zmq_socket):
        import msgpack

        await process_images(mock_zmq_socket, cycles=1, frames=1, pause=0)
        first_call_args = mock_zmq_socket.send.call_args_list[0][0][0]
        unpacked = msgpack.unpackb(first_call_args, raw=False)
        assert unpacked["msg_type"] == "start"


class TestMainCli:
    def test_main_calls_asyncio_run(self):
        from arroyosas.app.frame_listener_sim_cli import app
        from typer.testing import CliRunner

        runner = CliRunner()
        with (
            patch("arroyosas.app.frame_listener_sim_cli.asyncio.run") as mock_run,
            patch("arroyosas.app.frame_listener_sim_cli.settings") as mock_settings,
        ):
            mock_settings.tiled_poller.zmq_frame_publisher.address = "tcp://localhost:5556"
            mock_run.return_value = None
            runner.invoke(app, ["--cycles", "1", "--frames", "1", "--pause", "0"])
            mock_run.assert_called_once()
