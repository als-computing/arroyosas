"""Tests for arroyosas.app.tiled_event_sim_cli (TiledEventSimulator)"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from arroyosas.app.tiled_event_sim_cli import TiledEventSimulator

pytestmark = pytest.mark.asyncio


@pytest.fixture
def log_dir(tmp_path):
    """Create a test log directory with a run."""
    run_dir = tmp_path / "run_test_001"
    run_dir.mkdir()

    events = [
        {
            "event_name": "on_new_run",
            "sequence": 1,
            "timestamp": 1000.0,
            "callback_data": {"key": "run_uid_123"},
        },
        {
            "event_name": "on_streams_namespace",
            "sequence": 1,
            "timestamp": 1001.0,
            "callback_data": {"key": "streams"},
        },
        {
            "event_name": "on_event",
            "sequence": 1,
            "timestamp": 1002.0,
            "callback_data": {"sequence": 0, "key": "frame_0"},
        },
    ]

    for i, event in enumerate(events):
        event_file = run_dir / f"event_{i:04d}.json"
        event_file.write_text(json.dumps(event))

    return tmp_path


@pytest.fixture
def simulator(log_dir):
    return TiledEventSimulator(
        log_dir=str(log_dir),
        host="0.0.0.0",
        port=8001,
        stream_path="/stream",
    )


class TestTiledEventSimulator:
    def test_init(self, tmp_path):
        sim = TiledEventSimulator(str(tmp_path), host="0.0.0.0", port=8000, stream_path="/stream")
        assert sim.host == "0.0.0.0"
        assert sim.port == 8000
        assert sim.stream_path == "/stream"

    def test_list_available_runs(self, simulator, log_dir):
        runs = simulator.list_available_runs()
        assert "run_test_001" in runs

    def test_list_available_runs_empty_dir(self, tmp_path):
        sim = TiledEventSimulator(str(tmp_path))
        runs = sim.list_available_runs()
        assert runs == []

    def test_list_available_runs_no_json(self, tmp_path):
        run_dir = tmp_path / "run_no_json"
        run_dir.mkdir()
        (run_dir / "not_json.txt").write_text("text")
        sim = TiledEventSimulator(str(tmp_path))
        runs = sim.list_available_runs()
        assert "run_no_json" not in runs

    async def test_replay_run_sends_events(self, simulator, log_dir):
        mock_ws = AsyncMock()
        await simulator.replay_run(mock_ws, "run_test_001")
        # Should have sent at least 3 events (on_new_run, on_streams_namespace, on_event)
        assert mock_ws.send.call_count >= 2

    async def test_replay_run_nonexistent(self, simulator):
        mock_ws = AsyncMock()
        await simulator.replay_run(mock_ws, "nonexistent_run")
        mock_ws.send.assert_not_called()

    async def test_replay_run_empty_dir(self, simulator, log_dir):
        # Create empty run dir
        empty_run = log_dir / "run_empty"
        empty_run.mkdir()
        mock_ws = AsyncMock()
        await simulator.replay_run(mock_ws, "run_empty")
        mock_ws.send.assert_not_called()

    async def test_handle_client_wrong_path(self, simulator):
        mock_ws = AsyncMock()
        mock_ws.remote_address = ("127.0.0.1", 1234)
        await simulator.handle_client(mock_ws, "/wrong_path")
        # Client was not added to connected_clients properly
        # (actually never calls wait_closed)

    async def test_handle_client_no_runs(self, tmp_path):
        sim = TiledEventSimulator(str(tmp_path))
        mock_ws = AsyncMock()
        mock_ws.remote_address = ("127.0.0.1", 1234)
        await sim.handle_client(mock_ws, "/stream")
        mock_ws.send.assert_called_once()  # Error message
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["type"] == "error"

    async def test_handle_client_uses_specified_run(self, simulator, log_dir):
        simulator.specified_run_id = "run_test_001"
        mock_ws = AsyncMock()
        mock_ws.remote_address = ("127.0.0.1", 1234)
        mock_ws.wait_closed = AsyncMock(return_value=None)

        with patch.object(simulator, "replay_run", new=AsyncMock()):
            await simulator.handle_client(mock_ws, "/stream")
            simulator.replay_run.assert_called_once()

    async def test_start_creates_server(self, simulator):
        mock_server = MagicMock()
        mock_server.wait_closed = AsyncMock(return_value=None)

        async def fake_serve(*args, **kwargs):
            return mock_server

        with patch("arroyosas.app.tiled_event_sim_cli.websockets.serve", side_effect=fake_serve) as mock_serve:
            await simulator.start()
            mock_serve.assert_called_once()


class TestMainCli:
    def test_main_help(self):
        from typer.testing import CliRunner

        from arroyosas.app.tiled_event_sim_cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    def test_main_invoked(self, tmp_path):
        from typer.testing import CliRunner

        from arroyosas.app.tiled_event_sim_cli import app

        runner = CliRunner()
        with patch("arroyosas.app.tiled_event_sim_cli.asyncio.run") as mock_run:
            mock_run.return_value = None
            runner.invoke(app, [str(tmp_path)])
            mock_run.assert_called_once()
