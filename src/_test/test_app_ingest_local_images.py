"""Tests for arroyosas.app.ingest_local_images"""

import json
from unittest.mock import MagicMock, patch

import numpy as np

from arroyosas.app.ingest_local_images import (
    ingest_to_tiled,
    load_image_files,
    read_image_file,
    save_url_to_file,
)


class TestLoadImageFiles:
    def test_finds_jpg_files(self, tmp_path):
        (tmp_path / "img1.jpg").write_bytes(b"fake")
        (tmp_path / "img2.jpg").write_bytes(b"fake")
        (tmp_path / "data.txt").write_text("ignore")

        result = load_image_files(str(tmp_path))
        assert len(result) == 2
        assert all(".jpg" in f or ".JPG" in f for f in result)

    def test_finds_multiple_extensions(self, tmp_path):
        (tmp_path / "a.png").write_bytes(b"fake")
        (tmp_path / "b.tiff").write_bytes(b"fake")
        (tmp_path / "c.jpeg").write_bytes(b"fake")

        result = load_image_files(str(tmp_path))
        assert len(result) == 3

    def test_returns_sorted(self, tmp_path):
        (tmp_path / "z_img.png").write_bytes(b"fake")
        (tmp_path / "a_img.png").write_bytes(b"fake")

        result = load_image_files(str(tmp_path))
        assert result == sorted(result)

    def test_empty_folder(self, tmp_path):
        result = load_image_files(str(tmp_path))
        assert result == []


class TestReadImageFile:
    def test_read_grayscale_image(self, tmp_path):
        from PIL import Image

        img = Image.fromarray(np.zeros((10, 10), dtype=np.uint8), mode="L")
        path = str(tmp_path / "gray.png")
        img.save(path)

        result = read_image_file(path)
        assert isinstance(result, np.ndarray)

    def test_read_rgb_image_converts_to_uint32(self, tmp_path):
        from PIL import Image

        img = Image.fromarray(np.zeros((5, 5, 3), dtype=np.uint8), mode="RGB")
        path = str(tmp_path / "rgb.png")
        img.save(path)

        result = read_image_file(path)
        assert isinstance(result, np.ndarray)
        assert result.dtype == np.uint32

    def test_returns_zeros_on_error(self):
        result = read_image_file("/nonexistent/path/image.jpg")
        assert result is not None
        assert isinstance(result, np.ndarray)
        assert result.shape == (10, 10)


class TestSaveUrlToFile:
    def test_saves_url_and_metadata(self, tmp_path):
        url_file = str(tmp_path / "tiled_url.json")
        save_url_to_file("http://example.com/tiled", url_file, {"key": "val"})

        with open(url_file) as f:
            data = json.load(f)

        assert data["tiled_url"] == "http://example.com/tiled"
        assert "timestamp" in data
        assert data["metadata"]["key"] == "val"

    def test_saves_with_none_metadata(self, tmp_path):
        url_file = str(tmp_path / "url.json")
        save_url_to_file("http://example.com", url_file)

        with open(url_file) as f:
            data = json.load(f)
        assert data["metadata"] == {}

    def test_handles_write_error(self, tmp_path):
        # Writing to a directory (not a file) should not raise
        save_url_to_file("http://example.com", "/nonexistent/dir/file.json")


class TestIngestToTiled:
    async def test_ingest_creates_container_and_writes_arrays(self, tmp_path):
        from PIL import Image

        # Create test images
        for i in range(3):
            img = Image.fromarray(np.zeros((10, 10), dtype=np.uint8), mode="L")
            img.save(str(tmp_path / f"image_{i:04d}.png"))

        image_files = [str(tmp_path / f"image_{i:04d}.png") for i in range(3)]

        mock_container = MagicMock()
        mock_client = MagicMock()
        mock_client.create_container.return_value = mock_container
        mock_client.uri = "http://tiled:8000/api/v1/metadata"

        tiled_url, metadata = await ingest_to_tiled(mock_client, "test_container", image_files)

        assert "tiled_url" in tiled_url or tiled_url is not None
        assert metadata["num_images"] == 3
        assert mock_container.write_array.call_count >= 3  # one per image + metadata

    async def test_ingest_container_already_exists(self, tmp_path):
        from PIL import Image

        img = Image.fromarray(np.zeros((5, 5), dtype=np.uint8), mode="L")
        img.save(str(tmp_path / "img.png"))

        mock_container = MagicMock()
        mock_client = MagicMock()
        mock_client.create_container.side_effect = Exception("Already exists")
        mock_client.__getitem__ = MagicMock(return_value=mock_container)
        mock_client.uri = "http://tiled:8000/api/v1/metadata"

        tiled_url, metadata = await ingest_to_tiled(mock_client, "existing_container", [str(tmp_path / "img.png")])
        assert tiled_url is not None


class TestMainCli:
    def test_main_help(self):
        from typer.testing import CliRunner

        from arroyosas.app.ingest_local_images import app

        runner = CliRunner()
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    def test_main_missing_folder(self, tmp_path):
        from typer.testing import CliRunner

        from arroyosas.app.ingest_local_images import app

        runner = CliRunner()
        with patch("arroyosas.app.ingest_local_images.asyncio.run") as mock_run:
            mock_run.return_value = None
            runner.invoke(
                app,
                [
                    "--image-folder",
                    str(tmp_path / "nonexistent"),
                    "--url-file",
                    str(tmp_path / "out.json"),
                ],
            )
            mock_run.assert_called_once()

    def test_main_no_images(self, tmp_path):
        from typer.testing import CliRunner

        from arroyosas.app.ingest_local_images import app

        runner = CliRunner()
        with patch("arroyosas.app.ingest_local_images.asyncio.run") as mock_run:
            mock_run.return_value = None
            runner.invoke(
                app,
                [
                    "--image-folder",
                    str(tmp_path),  # Empty folder
                    "--url-file",
                    str(tmp_path / "out.json"),
                ],
            )
            mock_run.assert_called_once()
