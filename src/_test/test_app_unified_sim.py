"""Tests for arroyosas.app.unified_sim_cli (unified simulator)"""

import json
from unittest.mock import MagicMock, patch

import aiosqlite
import pytest

from arroyosas.app.unified_sim_cli import (
    get_matching_keys,
    get_num_frames,
    get_urls_from_db,
    load_url_from_file,
    transform_url_for_env,
)


class TestTransformUrlForEnv:
    def test_dev_url_unchanged(self):
        url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/uuid-123/primary/data/img"
        result = transform_url_for_env(url, "dev")
        assert result == url

    def test_transform_to_prod(self):
        url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/uuid-123/primary/data/pil_img"
        result = transform_url_for_env(url, "prod")
        assert "nsls2.bnl.gov" in result
        assert "smi/raw" in result

    def test_unknown_env_falls_back_to_dev(self):
        url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/uuid-123/data/img"
        # Unknown env
        result = transform_url_for_env(url, "staging")
        # Should not crash
        assert result is not None

    def test_invalid_url_returns_original(self):
        url = "http://example.com/no_array_full"
        result = transform_url_for_env(url, "prod")
        assert result == url

    def test_preserves_slice_param(self):
        url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/uuid-abc/primary/data/img?slice=0:1"
        result = transform_url_for_env(url, "dev")
        # Dev stays the same
        assert "slice=0:1" in result


class TestGetUrlsFromDb:
    async def test_get_urls_from_db(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, tiled_url TEXT, feature_vector TEXT)")
            await conn.execute("INSERT INTO vectors (tiled_url, feature_vector) VALUES (?, ?)", ("http://url1", "[]"))
            await conn.execute("INSERT INTO vectors (tiled_url, feature_vector) VALUES (?, ?)", ("http://url2", "[]"))
            await conn.commit()

        results = await get_urls_from_db(db_path)
        assert len(results) == 2

    async def test_get_urls_from_db_with_limit(self, tmp_path):
        db_path = str(tmp_path / "limit_test.db")
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, tiled_url TEXT, feature_vector TEXT)")
            for i in range(5):
                await conn.execute("INSERT INTO vectors (tiled_url, feature_vector) VALUES (?, ?)", (f"http://url{i}", "[]"))
            await conn.commit()

        results = await get_urls_from_db(db_path, limit=3)
        assert len(results) == 3

    async def test_get_urls_from_db_empty(self, tmp_path):
        db_path = str(tmp_path / "empty.db")
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, tiled_url TEXT, feature_vector TEXT)")
            await conn.commit()

        results = await get_urls_from_db(db_path)
        assert results == []

    async def test_get_urls_from_db_error(self):
        results = await get_urls_from_db("/nonexistent/path/db.sqlite")
        assert results == []


class TestLoadUrlFromFile:
    def test_load_valid_url_file(self, tmp_path):
        url_file = tmp_path / "url.json"
        data = {
            "tiled_url": "http://example.com/tiled",
            "timestamp": "2024-01-01",
            "metadata": {"key": "val"},
        }
        url_file.write_text(json.dumps(data))

        tiled_url, metadata = load_url_from_file(str(url_file))
        assert tiled_url == "http://example.com/tiled"
        assert metadata["key"] == "val"

    def test_load_nonexistent_file(self, tmp_path):
        tiled_url, metadata = load_url_from_file(str(tmp_path / "nonexistent.json"))
        assert tiled_url is None
        assert metadata is None

    def test_load_file_missing_url(self, tmp_path):
        url_file = tmp_path / "no_url.json"
        url_file.write_text(json.dumps({"metadata": {}}))
        tiled_url, metadata = load_url_from_file(str(url_file))
        assert tiled_url is None

    def test_load_invalid_json(self, tmp_path):
        url_file = tmp_path / "invalid.json"
        url_file.write_text("not json")
        tiled_url, metadata = load_url_from_file(str(url_file))
        assert tiled_url is None


class TestGetMatchingKeys:
    async def test_get_matching_keys(self):
        mock_container = MagicMock()
        all_keys = ["run_0001_0001", "run_0001_0002", "run_0001_metadata", "run_0002_0001"]
        mock_container.keys.return_value = all_keys
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_container)

        result = await get_matching_keys(mock_client, "container", "run_0001_[0-9]{4}")
        assert "run_0001_0001" in result
        assert "run_0001_0002" in result
        assert "run_0001_metadata" not in result

    async def test_get_matching_keys_error(self):
        mock_client = MagicMock()
        mock_client.__getitem__.side_effect = Exception("Error")
        result = await get_matching_keys(mock_client, "container", "pattern")
        assert result == []


class TestGetNumFrames:
    def test_get_num_frames_with_shape(self):
        mock_client = MagicMock()
        mock_client.shape = (10, 100, 100)
        with patch("arroyosas.app.unified_sim_cli.from_uri", return_value=mock_client):
            result = get_num_frames("http://tiled:8000/api/v1")
            assert result == 10

    def test_get_num_frames_no_shape(self):
        mock_client = MagicMock(spec=[])  # No shape attribute
        with patch("arroyosas.app.unified_sim_cli.from_uri", return_value=mock_client):
            result = get_num_frames("http://tiled:8000/api/v1")
            assert result == 0


class TestMainCli:
    def test_main_help(self):
        from typer.testing import CliRunner

        from arroyosas.app.unified_sim_cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    def test_main_direct_mode(self):
        from typer.testing import CliRunner

        from arroyosas.app.unified_sim_cli import app

        runner = CliRunner()
        with (
            patch("arroyosas.app.unified_sim_cli.asyncio.run") as mock_run,
            patch("arroyosas.app.unified_sim_cli.settings") as mock_settings,
        ):
            mock_settings.tiled_poller.zmq_frame_publisher.address = "tcp://localhost:5556"
            mock_run.return_value = None
            runner.invoke(app, ["--mode", "direct"])
            mock_run.assert_called_once()

    def test_main_db_replay_no_db(self, tmp_path):
        from typer.testing import CliRunner

        from arroyosas.app.unified_sim_cli import app

        runner = CliRunner()
        with (
            patch("arroyosas.app.unified_sim_cli.asyncio.run") as mock_run,
            patch("arroyosas.app.unified_sim_cli.settings") as mock_settings,
        ):
            mock_settings.tiled_poller.zmq_frame_publisher.address = "tcp://localhost:5556"
            mock_run.return_value = None
            runner.invoke(
                app,
                ["--mode", "db_replay", "--db-path", str(tmp_path / "missing.db")],
            )
            mock_run.assert_called_once()

    def test_main_local_tiled_mode(self, tmp_path):
        from typer.testing import CliRunner

        from arroyosas.app.unified_sim_cli import app

        runner = CliRunner()
        with (
            patch("arroyosas.app.unified_sim_cli.asyncio.run") as mock_run,
            patch("arroyosas.app.unified_sim_cli.settings") as mock_settings,
        ):
            mock_settings.tiled_poller.zmq_frame_publisher.address = "tcp://localhost:5556"
            mock_run.return_value = None
            runner.invoke(
                app,
                ["--mode", "local_tiled", "--url-file", str(tmp_path / "missing.json")],
            )
            mock_run.assert_called_once()

    def test_main_unknown_mode(self):
        from typer.testing import CliRunner

        from arroyosas.app.unified_sim_cli import app

        runner = CliRunner()
        with (
            patch("arroyosas.app.unified_sim_cli.asyncio.run") as mock_run,
            patch("arroyosas.app.unified_sim_cli.settings") as mock_settings,
        ):
            mock_settings.tiled_poller.zmq_frame_publisher.address = "tcp://localhost:5556"
            mock_run.return_value = None
            runner.invoke(app, ["--mode", "unknown_mode"])
            mock_run.assert_called_once()
