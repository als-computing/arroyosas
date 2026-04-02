"""Tests for arroyosas.tiled.ingestor (TiledIngestor, parse_txt_accompanying_edf)"""
import os
import pathlib
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from arroyosas.tiled.ingestor import TiledIngestor, parse_txt_accompanying_edf


class TestParseTxtAccompanyingEdf:
    def test_with_nonexistent_file(self, tmp_path):
        edf_path = str(tmp_path / "scan.edf")
        # txt file doesn't exist
        result = parse_txt_accompanying_edf(edf_path)
        assert result == {}

    def test_with_string_path(self, tmp_path):
        edf_path = str(tmp_path / "scan.edf")
        txt_path = str(tmp_path / "scan.txt")
        with open(txt_path, "w") as f:
            f.write("key1: value1\n")
            f.write("key2: value2\n")

        result = parse_txt_accompanying_edf(edf_path)
        assert result["key1"] == "value1"
        assert result["key2"] == "value2"

    def test_with_pathlib_path(self, tmp_path):
        edf_path = tmp_path / "scan.edf"
        txt_path = tmp_path / "scan.txt"
        txt_path.write_text("energy: 10.5\n")

        result = parse_txt_accompanying_edf(edf_path)
        assert "energy" in result
        assert result["energy"] == "10.5"

    def test_with_keyless_lines(self, tmp_path):
        edf_path = str(tmp_path / "scan.edf")
        txt_path = str(tmp_path / "scan.txt")
        with open(txt_path, "w") as f:
            f.write("some_value_without_colon\n")
            f.write("key: val\n")

        result = parse_txt_accompanying_edf(edf_path)
        assert "Keyless Parameter #0" in result
        assert result["Keyless Parameter #0"] == "some_value_without_colon"

    def test_ignores_exclamation_lines(self, tmp_path):
        edf_path = str(tmp_path / "scan.edf")
        txt_path = str(tmp_path / "scan.txt")
        with open(txt_path, "w") as f:
            f.write("!0\n")
            f.write("key: val\n")

        result = parse_txt_accompanying_edf(edf_path)
        # "!0" lines are ignored
        assert "Keyless Parameter #0" not in result

    def test_multiple_colons_in_value(self, tmp_path):
        edf_path = str(tmp_path / "scan.edf")
        txt_path = str(tmp_path / "scan.txt")
        with open(txt_path, "w") as f:
            f.write("timestamp: 2024-01-01 12:00:00\n")

        result = parse_txt_accompanying_edf(edf_path)
        # maxsplit=1 so value includes everything after first colon
        assert result["timestamp"] == "2024-01-01 12:00:00"

    def test_empty_txt_file(self, tmp_path):
        edf_path = str(tmp_path / "scan.edf")
        txt_path = str(tmp_path / "scan.txt")
        txt_path_obj = pathlib.Path(txt_path)
        txt_path_obj.write_text("")

        result = parse_txt_accompanying_edf(edf_path)
        assert result == {}


class TestTiledIngestor:
    def _make_mock_client(self):
        client = MagicMock()
        container = MagicMock()
        container.__contains__ = MagicMock(return_value=False)
        container.__getitem__ = MagicMock(return_value=MagicMock())
        container.create_container.return_value = MagicMock()
        client.__getitem__ = MagicMock(return_value=container)
        return client, container

    def test_add_scan_tiled_edf(self, tmp_path):
        client, container = self._make_mock_client()
        # Build nested structure
        sub_container = MagicMock()
        sub_container.__contains__ = MagicMock(return_value=False)
        container.create_container.return_value = sub_container
        sub_container.__contains__ = MagicMock(return_value=False)
        scan_client = MagicMock()
        scan_client.uri = "http://example.com/scan"
        sub_container.new.return_value = scan_client
        container.__contains__ = MagicMock(return_value=False)

        # Create a fake edf file and txt file
        edf_file = tmp_path / "scan1.edf"
        edf_file.write_bytes(b"fake edf data")
        txt_file = tmp_path / "scan1.txt"
        txt_file.write_text("energy: 10.5\n")

        ingestor = TiledIngestor(
            tiled_client=client,
            raw_tiled_root="raw",
            path_to_raw_data=str(tmp_path),
        )

        uri = ingestor.add_scan_tiled(str(edf_file))
        assert uri == "http://example.com/scan"

    def test_add_scan_tiled_gb(self, tmp_path):
        client, container = self._make_mock_client()
        scan_client = MagicMock()
        scan_client.uri = "http://example.com/scan.gb"
        sub_container = MagicMock()
        sub_container.__contains__ = MagicMock(return_value=False)
        sub_container.new.return_value = scan_client
        container.create_container.return_value = sub_container
        container.__contains__ = MagicMock(return_value=False)

        gb_file = tmp_path / "scan1.gb"
        gb_file.write_bytes(b"fake gb data")

        ingestor = TiledIngestor(
            tiled_client=client,
            raw_tiled_root="raw",
            path_to_raw_data=str(tmp_path),
        )
        uri = ingestor.add_scan_tiled(str(gb_file))
        assert uri == "http://example.com/scan.gb"

    def test_add_scan_tiled_existing_key_deleted(self, tmp_path):
        client = MagicMock()
        root_container = MagicMock()
        root_container.__contains__ = MagicMock(return_value=False)
        sub_container = MagicMock()
        sub_container.__contains__ = MagicMock(return_value=True)  # Key exists
        scan_client = MagicMock()
        scan_client.uri = "http://example.com/scan"
        sub_container.new.return_value = scan_client
        root_container.create_container.return_value = sub_container
        client.__getitem__ = MagicMock(return_value=root_container)

        edf_file = tmp_path / "existing.edf"
        edf_file.write_bytes(b"fake")

        ingestor = TiledIngestor(
            tiled_client=client,
            raw_tiled_root="raw",
            path_to_raw_data=str(tmp_path),
        )
        ingestor.add_scan_tiled(str(edf_file))
        sub_container.delete.assert_called_once_with("existing")

    def test_add_scan_tiled_existing_container_reused(self, tmp_path):
        client = MagicMock()
        root_container = MagicMock()
        existing_sub = MagicMock()
        existing_sub.__contains__ = MagicMock(return_value=False)
        scan_client = MagicMock()
        scan_client.uri = "http://example.com/existing"
        existing_sub.new.return_value = scan_client
        root_container.__contains__ = MagicMock(return_value=True)
        root_container.__getitem__ = MagicMock(return_value=existing_sub)
        client.__getitem__ = MagicMock(return_value=root_container)

        edf_file = tmp_path / "scan.edf"
        edf_file.write_bytes(b"data")

        ingestor = TiledIngestor(
            tiled_client=client,
            raw_tiled_root="raw",
            path_to_raw_data=str(tmp_path),
        )
        uri = ingestor.add_scan_tiled(str(edf_file))
        # Should reuse existing container, not create new one
        root_container.create_container.assert_not_called()
