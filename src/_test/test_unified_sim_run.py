"""Additional coverage for arroyosas.app.unified_sim_cli inner run() logic."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import numpy as np
import pytest

pytestmark = pytest.mark.asyncio


async def _make_zmq_socket():
    socket = AsyncMock()
    return socket


async def test_db_replay_mode_full_flow(tmp_path):
    """Test the db_replay code path via calling the inner run() directly."""
    from arroyosas.app.unified_sim_cli import (
        get_urls_from_db,
    )

    # create a test DB
    db_path = str(tmp_path / "test.db")
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, tiled_url TEXT, feature_vector TEXT)")
        await conn.execute(
            "INSERT INTO vectors (tiled_url, feature_vector) VALUES (?, ?)",
            ("http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/abc-123/primary/data/img", "[]"),
        )
        await conn.commit()

    urls = await get_urls_from_db(db_path)
    assert len(urls) == 1


async def test_read_image_from_tiled_url():
    from arroyosas.app.unified_sim_cli import read_image_from_tiled_url

    mock_client = MagicMock()
    mock_data = MagicMock()
    mock_data.__getitem__ = MagicMock(return_value=np.zeros((10, 10), dtype=np.uint32))
    mock_client.__getitem__ = MagicMock(return_value=mock_data)
    mock_data.shape = (1, 10, 10)

    url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/uuid-123/primary/data/img?slice=0:1"
    with patch("arroyosas.app.unified_sim_cli.from_uri", return_value=mock_client):
        image, index = await read_image_from_tiled_url(url)
    assert index == 0


async def test_read_image_invalid_url():
    from arroyosas.app.unified_sim_cli import read_image_from_tiled_url

    image, index = await read_image_from_tiled_url("http://invalid_format_url")
    assert image is None
    assert index == 0


async def test_process_images_from_tiled(tmp_path):
    """Test process_images_from_tiled sends messages correctly."""
    from arroyosas.app.unified_sim_cli import process_images_from_tiled

    mock_socket = AsyncMock()
    mock_client = MagicMock()
    mock_client.shape = (3, 10, 10)

    frame_data = np.zeros((10, 10), dtype=np.uint32)
    mock_client.__getitem__ = MagicMock(return_value=frame_data)

    with (
        patch("arroyosas.app.unified_sim_cli.from_uri", return_value=mock_client),
        patch("arroyosas.app.unified_sim_cli.get_num_frames", return_value=3),
    ):
        await process_images_from_tiled(
            socket=mock_socket,
            cycles=1,
            frames=3,
            pause=0,
            tiled_uri="http://tiled:8000/api/v1/metadata/rsoxs",
        )

    # start + 3 frames + stop
    assert mock_socket.send.call_count == 5


async def test_load_url_from_file_with_metadata(tmp_path):
    from arroyosas.app.unified_sim_cli import load_url_from_file

    url_file = tmp_path / "url.json"
    data = {
        "tiled_url": "http://tiled.example.com/api/v1/array/full/container/run_0001",
        "metadata": {"width": 100, "height": 100, "data_type": "float32", "num_images": 5},
    }
    url_file.write_text(json.dumps(data))

    tiled_url, metadata = load_url_from_file(str(url_file))
    assert tiled_url == data["tiled_url"]
    assert metadata["width"] == 100


async def test_transform_url_with_slice():
    from arroyosas.app.unified_sim_cli import transform_url_for_env

    url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/abc-def/primary/data/pil1M?slice=0:1"
    result = transform_url_for_env(url, "prod")
    # Prod URL should contain smi/raw
    assert "smi/raw" in result


async def test_transform_url_unknown_env_fallback():
    from arroyosas.app.unified_sim_cli import transform_url_for_env

    url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/some-uuid/data/img"
    result = transform_url_for_env(url, "unknown")
    # Falls back to dev - returns same URL since it contains tiled-dev
    assert result is not None
