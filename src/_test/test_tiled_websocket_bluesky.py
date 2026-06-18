"""Tests for arroyosas.tiled.tiled_websocket_bluesky (TiledClientListener)"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from arroyosas.schemas import RawFrameEvent, SASStart
from arroyosas.tiled.tiled_websocket_bluesky import TiledClientListener, tiled_ws_listener_factory


@pytest.fixture
def mock_tiled_client():
    client = MagicMock()
    client.context = MagicMock()
    return client


@pytest.fixture
def listener(mock_tiled_client, tmp_path):
    return TiledClientListener(
        tiled_client=mock_tiled_client,
        stream_name="primary",
        target="img",
        create_run_logs=True,
        log_dir=str(tmp_path / "tiled_logs"),
    )


class TestTiledClientListenerBluesky:
    def test_init(self, mock_tiled_client, tmp_path):
        log_dir = str(tmp_path / "logs")
        listener = TiledClientListener(
            tiled_client=mock_tiled_client,
            stream_name="primary",
            log_dir=log_dir,
        )
        assert listener.tiled_client is mock_tiled_client
        assert listener.stream_name == "primary"
        assert listener.target == "img"
        assert os.path.exists(log_dir)

    def test_init_creates_log_dir(self, mock_tiled_client, tmp_path):
        log_dir = str(tmp_path / "new_logs")
        assert not os.path.exists(log_dir)
        TiledClientListener(mock_tiled_client, "primary", log_dir=log_dir)
        assert os.path.exists(log_dir)

    def test_create_run_folder(self, listener, tmp_path):
        folder = listener.create_run_folder("run_abc123")
        assert os.path.exists(folder)
        assert "run_abc123" in folder
        assert listener.current_run_dir == folder
        assert len(listener.event_counters) == 0  # reset

    def test_log_message_to_json_creates_file(self, listener, tmp_path):
        listener.create_run_folder("run_log_test")
        sub_mock = MagicMock()
        sub_mock.segments = ["uid1"]
        listener.log_message_to_json("on_new_run", sub_mock, {"key": "value"})

        log_files = list(Path(listener.current_run_dir).glob("on_new_run_*.json"))
        assert len(log_files) == 1

        with open(log_files[0]) as f:
            data = json.load(f)
        assert data["event_name"] == "on_new_run"
        assert data["callback_data"] == {"key": "value"}

    def test_log_message_to_json_sequence_increments(self, listener):
        listener.create_run_folder("run_seq")
        sub_mock = MagicMock()
        sub_mock.segments = []
        listener.log_message_to_json("on_new_run", sub_mock, {})
        listener.log_message_to_json("on_new_run", sub_mock, {})

        assert listener.event_counters["on_new_run"] == 2

    def test_log_message_to_json_no_current_dir(self, listener):
        # Should silently return if no current_run_dir
        listener.current_run_dir = None
        listener.log_message_to_json("on_new_run", MagicMock(), {})
        # No file created, no error

    def test_on_new_run_creates_subscription(self, listener, mock_tiled_client):
        update = MagicMock()
        update.key = "run_uid_123"
        update.subscription = MagicMock()
        update.model_dump.return_value = {"key": "run_uid_123"}

        run_node = MagicMock()
        run_sub = MagicMock()
        run_sub.child_created = MagicMock()
        run_node.subscribe.return_value = run_sub
        update.child.return_value = run_node

        with patch.object(listener, "publish_start") as mock_publish_start:
            listener.on_new_run(update)

        run_node.subscribe.assert_called_once_with(start=0)
        run_sub.child_created.add_callback.assert_called_once_with(listener.on_streams_namespace)
        run_sub.start.assert_called_once()
        mock_publish_start.assert_called_once_with(run_node, {"key": "run_uid_123"})

    def test_on_new_run_calls_publish_start(self, listener):
        update = MagicMock()
        update.key = "run_001"
        update.subscription = MagicMock()
        update.model_dump.return_value = {"key": "run_001"}

        run_node = MagicMock()
        run_sub = MagicMock()
        run_sub.child_created = MagicMock()
        run_node.subscribe.return_value = run_sub
        update.child.return_value = run_node

        with patch.object(listener, "publish_start") as mock_pub:
            listener.on_new_run(update)

        mock_pub.assert_called_once_with(run_node, {"key": "run_001"})

    def test_on_streams_namespace(self, listener, mock_tiled_client):
        update = MagicMock()
        update.key = "streams"
        update.subscription = MagicMock()
        update.model_dump.return_value = {"key": "streams"}

        streams_node = MagicMock()
        streams_sub = MagicMock()
        streams_sub.child_created = MagicMock()
        streams_node.subscribe.return_value = streams_sub
        update.child.return_value = streams_node

        listener.on_streams_namespace(update)

        streams_node.subscribe.assert_called_once_with(start=0)
        streams_sub.child_created.add_callback.assert_called_once_with(listener.on_new_stream)

    def test_on_new_stream(self, listener):
        update = MagicMock()
        update.key = "primary"
        update.subscription = MagicMock()
        update.model_dump.return_value = {"key": "primary"}

        stream_node = MagicMock()
        stream_sub = MagicMock()
        stream_sub.child_created = MagicMock()
        stream_node.subscribe.return_value = stream_sub
        update.child.return_value = stream_node

        listener.on_new_stream(update)

        stream_node.subscribe.assert_called_once_with(start=0)
        stream_sub.child_created.add_callback.assert_called_once_with(listener.on_node_in_stream)

    def test_on_node_in_stream_matching_target(self, listener):
        update = MagicMock()
        update.key = "img"
        update.subscription = MagicMock()
        update.model_dump.return_value = {"key": "img", "sequence": 0}

        data_node = MagicMock()
        data_sub = MagicMock()
        data_sub.new_data = MagicMock()
        data_node.subscribe.return_value = data_sub
        update.child.return_value = data_node

        listener.on_node_in_stream(update)

        data_node.subscribe.assert_called_once_with(start=0)
        data_sub.new_data.add_callback.assert_called_once_with(listener.on_event)
        data_sub.start.assert_called_once()

    def test_on_node_in_stream_non_matching_target(self, listener):
        update = MagicMock()
        update.key = "other_key"
        update.subscription = MagicMock()
        update.model_dump.return_value = {"key": "other_key", "sequence": 0}

        listener.on_node_in_stream(update)

        update.child.assert_not_called()

    def test_on_event(self, listener):
        update = MagicMock()
        update.subscription = MagicMock()
        update.model_dump.return_value = {"sequence": 5}
        listener.create_run_folder("run_on_event")
        with patch.object(listener, "publish_event") as mock_publish_event:
            listener.on_event(update)

        # With create_run_logs=True, should log to JSON
        log_files = list(Path(listener.current_run_dir).glob("on_event_*.json"))
        assert len(log_files) == 1
        mock_publish_event.assert_called_once_with(update)

    def test_publish_start(self, listener):
        data = {"key": "run_001"}
        run_node = MagicMock()
        run_node.metadata = {"run_name": "Run 1", "width": 5, "height": 7, "data_type": "uint16"}

        with patch.object(listener, "send_to_operator") as mock_send:
            listener.publish_start(run_node, data)
            mock_send.assert_called_once()
            msg = mock_send.call_args[0][0]
            assert isinstance(msg, SASStart)
            assert msg.run_id == "run_001"
            assert msg.run_name == "Run 1"
            assert msg.width == 5
            assert msg.height == 7
            assert msg.data_type == "uint16"

    def test_publish_event(self, listener):
        update = MagicMock()
        update.data.return_value = np.zeros((5, 5))
        update.sequence = 7

        with patch.object(listener, "send_to_operator") as mock_send:
            listener.publish_event(update)
            mock_send.assert_called_once()
            msg = mock_send.call_args[0][0]
            assert isinstance(msg, RawFrameEvent)
            assert msg.frame_number == 7

    def test_print_event(self, listener, capsys):
        listener.print_event("test_event", {"data": "value"})
        captured = capsys.readouterr()
        assert "test_event" in captured.out


class TestCreateTiledWebsocketListenerBluesky:
    def test_create(self, tmp_path):
        with patch("arroyosas.tiled.tiled_websocket_bluesky.from_uri") as mock_from_uri:
            mock_client = MagicMock()
            mock_from_uri.return_value = mock_client
            listener = tiled_ws_listener_factory(
                uri="http://example.com",
                stream_name="primary",
                api_key="test_key",
                log_dir=str(tmp_path / "logs"),
            )
            assert isinstance(listener, TiledClientListener)
