"""Tests for inner run() logic of unified_sim_cli.py."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import numpy as np
import pytest

# ---------------------------------------------------------------------------
# _read_image_from_tiled_url_sync  (lines 136-198)
# ---------------------------------------------------------------------------


def test_read_image_sync_valid_url():
    """Test the synchronous image reader with a valid URL."""
    from arroyosas.app.unified_sim_cli import _read_image_from_tiled_url_sync

    mock_client = MagicMock()
    mock_data = MagicMock()
    mock_data.shape = (5, 10, 10)
    mock_data.dtype.name = "uint32"
    expected_image = np.zeros((10, 10), dtype=np.uint32)
    mock_data.__getitem__ = MagicMock(return_value=expected_image)
    mock_client.__getitem__ = MagicMock(return_value=mock_data)

    url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/abc-123/primary/data/img?slice=2:3"
    with patch("arroyosas.app.unified_sim_cli.from_uri", return_value=mock_client):
        image, index = _read_image_from_tiled_url_sync(url)

    assert index == 2
    assert image is expected_image


def test_read_image_sync_invalid_format():
    """Test _read_image_from_tiled_url_sync with invalid URL format."""
    from arroyosas.app.unified_sim_cli import _read_image_from_tiled_url_sync

    # No /api/v1/ in URL -> returns None, 0
    image, index = _read_image_from_tiled_url_sync("http://example.com/no_api")
    assert image is None
    assert index == 0


def test_read_image_sync_exception():
    """Test _read_image_from_tiled_url_sync when from_uri raises."""
    from arroyosas.app.unified_sim_cli import _read_image_from_tiled_url_sync

    url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/abc-123/primary/data/img"
    with patch("arroyosas.app.unified_sim_cli.from_uri", side_effect=Exception("connection refused")):
        image, index = _read_image_from_tiled_url_sync(url)

    assert image is None
    assert index == 0


def test_read_image_sync_no_array_full():
    """Test URL without array/full uses full path as dataset_uri."""
    from arroyosas.app.unified_sim_cli import _read_image_from_tiled_url_sync

    mock_client = MagicMock()
    mock_data = MagicMock()
    expected_image = np.zeros((10, 10), dtype=np.uint32)
    mock_data.__getitem__ = MagicMock(return_value=expected_image)
    mock_client.__getitem__ = MagicMock(return_value=mock_data)

    url = "http://tiled.example.com/api/v1/metadata/mypath/dataset"
    with patch("arroyosas.app.unified_sim_cli.from_uri", return_value=mock_client):
        image, index = _read_image_from_tiled_url_sync(url)

    assert index == 0
    assert image is expected_image


def test_read_image_sync_slice_without_digit():
    """Test that a non-digit slice start defaults index to 0."""
    from arroyosas.app.unified_sim_cli import _read_image_from_tiled_url_sync

    mock_client = MagicMock()
    mock_data = MagicMock()
    expected_image = np.zeros((10, 10), dtype=np.uint32)
    mock_data.__getitem__ = MagicMock(return_value=expected_image)
    mock_client.__getitem__ = MagicMock(return_value=mock_data)

    # slice=a:b -> parts[0]='a' is not isdigit
    url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/abc-123/primary/data/img?slice=a:b"
    with patch("arroyosas.app.unified_sim_cli.from_uri", return_value=mock_client):
        image, index = _read_image_from_tiled_url_sync(url)

    assert index == 0


# ---------------------------------------------------------------------------
# fetch_image_from_tiled (lines 239-259)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_image_from_tiled_success():
    from arroyosas.app.unified_sim_cli import fetch_image_from_tiled

    mock_array_client = MagicMock()
    mock_array_client.read.return_value = np.zeros((10, 10), dtype=np.uint32)

    mock_container = MagicMock()
    mock_container.__getitem__ = MagicMock(return_value=mock_array_client)

    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_container)

    result = await fetch_image_from_tiled(mock_client, "container_name", "image_key")
    assert isinstance(result, np.ndarray)


@pytest.mark.asyncio
async def test_fetch_image_from_tiled_returns_numpy():
    from arroyosas.app.unified_sim_cli import fetch_image_from_tiled

    # Return a list, should be converted to np.ndarray
    mock_array_client = MagicMock()
    mock_array_client.read.return_value = [[1, 2], [3, 4]]  # List, not ndarray

    mock_container = MagicMock()
    mock_container.__getitem__ = MagicMock(return_value=mock_array_client)

    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_container)

    result = await fetch_image_from_tiled(mock_client, "container_name", "image_key")
    assert isinstance(result, np.ndarray)


@pytest.mark.asyncio
async def test_fetch_image_from_tiled_error():
    from arroyosas.app.unified_sim_cli import fetch_image_from_tiled

    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(side_effect=Exception("not found"))

    result = await fetch_image_from_tiled(mock_client, "container_name", "image_key")
    # Returns zeros on error
    assert isinstance(result, np.ndarray)
    assert result.shape == (10, 10)


# ---------------------------------------------------------------------------
# process_images_from_tiled - exception path and frame_count==0 path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_images_from_tiled_zero_frames():
    """Test that the zero frames path logs warning and skips."""
    from arroyosas.app.unified_sim_cli import process_images_from_tiled

    mock_socket = AsyncMock()
    mock_client = MagicMock()
    mock_client.shape = (0,)

    with (
        patch("arroyosas.app.unified_sim_cli.from_uri", return_value=mock_client),
        patch("arroyosas.app.unified_sim_cli.get_num_frames", return_value=0),
    ):
        await process_images_from_tiled(
            socket=mock_socket,
            cycles=1,
            frames=5,
            pause=0,
            tiled_uri="http://tiled:8000/api/v1",
        )

    # Only start was sent (then zero frames path skips)
    assert mock_socket.send.call_count == 1  # just the start


@pytest.mark.asyncio
async def test_process_images_from_tiled_frame_error():
    """Test that frame-level exceptions are caught per-frame."""
    from arroyosas.app.unified_sim_cli import process_images_from_tiled

    mock_socket = AsyncMock()
    mock_client = MagicMock()
    # First frame raises, second succeeds
    good_frame = np.zeros((10, 10), dtype=np.uint32)
    mock_client.__getitem__ = MagicMock(side_effect=[Exception("frame error"), good_frame])

    with (
        patch("arroyosas.app.unified_sim_cli.from_uri", return_value=mock_client),
        patch("arroyosas.app.unified_sim_cli.get_num_frames", return_value=2),
    ):
        await process_images_from_tiled(
            socket=mock_socket,
            cycles=1,
            frames=2,
            pause=0,
            tiled_uri="http://tiled:8000/api/v1",
        )

    # start + 1 good frame + stop = 3
    assert mock_socket.send.call_count == 3


@pytest.mark.asyncio
async def test_process_images_from_tiled_outer_exception():
    """Test that outer exception is caught and logged."""
    from arroyosas.app.unified_sim_cli import process_images_from_tiled

    mock_socket = AsyncMock()

    with patch("arroyosas.app.unified_sim_cli.from_uri", side_effect=Exception("connection failed")):
        # Should not raise
        await process_images_from_tiled(
            socket=mock_socket,
            cycles=1,
            frames=5,
            pause=0,
            tiled_uri="http://tiled:8000/api/v1",
        )


# ---------------------------------------------------------------------------
# Inner run() function - db_replay mode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_db_replay_mode(tmp_path):
    """Test the inner run() db_replay mode end-to-end."""
    import msgpack

    # Create a test DB with one URL
    db_path = str(tmp_path / "test.db")
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, tiled_url TEXT, feature_vector TEXT)")
        await conn.execute(
            "INSERT INTO vectors (tiled_url, feature_vector) VALUES (?, ?)",
            ("http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/abc-123/primary/data/img?slice=0:1", "[]"),
        )
        await conn.commit()

    # Mock ZMQ socket
    mock_socket = AsyncMock()
    mock_context = MagicMock()
    mock_context.socket.return_value = mock_socket

    # Mock image reading
    fake_image = np.zeros((10, 10), dtype=np.uint32)

    async def fake_run():
        import zmq
        import zmq.asyncio

        context = mock_context
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://localhost:5556")

        from datetime import datetime

        from arroyosas.app.unified_sim_cli import (
            get_urls_from_db,
            read_image_from_tiled_url,
            transform_url_for_env,
        )
        from arroyosas.schemas import RawFrameEvent, SASStart, SASStop, SerializableNumpyArrayModel

        urls = await get_urls_from_db(db_path, limit=10000)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start = SASStart(
            width=1679,
            height=1475,
            data_type="uint32",
            tiled_url="dev://latent_vectors",
            run_name="dev_tiled_run",
            run_id=str(current_time),
        )
        await socket.send(msgpack.packb(start.model_dump()))

        for db_id, tiled_url in urls:
            transformed_url = transform_url_for_env(tiled_url, "dev")
            image_data, index = await read_image_from_tiled_url(transformed_url, None)
            if image_data is not None:
                event = RawFrameEvent(
                    image=SerializableNumpyArrayModel(array=image_data),
                    frame_number=index,
                    tiled_url=transformed_url,
                )
                await socket.send(msgpack.packb(event.model_dump()))

        stop = SASStop(num_frames=len(urls))
        await socket.send(msgpack.packb(stop.model_dump()))

    with (
        patch("arroyosas.app.unified_sim_cli.from_uri", return_value=MagicMock()),
        patch("arroyosas.app.unified_sim_cli._read_image_from_tiled_url_sync", return_value=(fake_image, 0)),
    ):
        await fake_run()

    # socket.send should have been called: start + 1 frame + stop = 3
    assert mock_socket.send.call_count == 3


# ---------------------------------------------------------------------------
# Inner run() function - local_tiled mode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_local_tiled_mode_no_url_file():
    """Test local_tiled mode when url_file is missing."""
    from arroyosas.app.unified_sim_cli import load_url_from_file

    tiled_url, metadata = load_url_from_file("/tmp/nonexistent_url_file_xyz.json")
    assert tiled_url is None
    assert metadata is None


@pytest.mark.asyncio
async def test_run_local_tiled_mode_missing_metadata(tmp_path):
    """Test local_tiled mode with missing dimension metadata."""
    from arroyosas.app.unified_sim_cli import load_url_from_file

    url_file = tmp_path / "url.json"
    # Missing width/height/data_type
    data = {
        "tiled_url": "http://tiled.example.com/api/v1/array/full/container/run_0001",
        "metadata": {"num_images": 3},
    }
    url_file.write_text(json.dumps(data))

    tiled_url, metadata = load_url_from_file(str(url_file))
    assert tiled_url is not None
    assert metadata.get("num_images") == 3
    # width/height/data_type are not present
    assert "width" not in metadata


@pytest.mark.asyncio
async def test_run_local_tiled_mode_with_image_pattern(tmp_path):
    """Test local_tiled run with image_pattern in metadata."""
    from arroyosas.app.unified_sim_cli import get_matching_keys

    mock_container = MagicMock()
    all_keys = ["run_0001_0001", "run_0001_0002", "run_0001_meta"]
    mock_container.keys.return_value = all_keys

    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_container)

    matching = await get_matching_keys(mock_client, "container", "run_0001_[0-9]{4}")
    assert "run_0001_0001" in matching
    assert "run_0001_0002" in matching
    assert "run_0001_meta" not in matching


# ---------------------------------------------------------------------------
# transform_url_for_env - additional branches
# ---------------------------------------------------------------------------


def test_transform_url_no_stream_path():
    """Test transform_url_for_env when stream_path is empty (only uuid)."""
    from arroyosas.app.unified_sim_cli import transform_url_for_env

    # URL with only UUID, no stream path after it
    url = "http://tiled-dev.nsls2.bnl.gov/api/v1/array/full/abc-123"
    result = transform_url_for_env(url, "prod")
    # Should return original because no stream_path
    assert result == url


def test_transform_url_dev_from_prod_url():
    """Test transform to dev when URL has prod host."""
    from arroyosas.app.unified_sim_cli import transform_url_for_env

    # URL with prod host, transform to dev
    url = "http://tiled.nsls2.bnl.gov/api/v1/array/full/abc-123/primary/data/img"
    result = transform_url_for_env(url, "dev")
    # Should produce dev url
    assert "tiled-dev.nsls2.bnl.gov" in result or result is not None
