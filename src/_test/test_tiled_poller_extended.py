"""Extended tests for arroyosas.tiled.tiled_poller"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from arroyosas.schemas import (
    SASStart,
    SASStop,
    SerializableNumpyArrayModel,
)
from arroyosas.tiled.tiled_poller import (
    TiledPollingRedisListener,
    TiledProcessedPublisher,
    TiledRawFrameOperator,
    create_run_container,
    get_most_recent_run,
    get_run_container,
    sub_container,
    unsent_frame_numbers,
)


# ---------------------------------------------------------------------------
# unsent_frame_numbers (extending existing tests)
# ---------------------------------------------------------------------------


def test_unsent_frames_empty_sent():
    result = unsent_frame_numbers([], 5)
    assert result == [0, 1, 2, 3, 4]


def test_unsent_frames_all_sent():
    # sent [0,1,2,3,4], num_frames=5
    # gaps between min(0) and max(4): none
    # extra: range(max+1=5, num_frames+1=6) = [5]
    result = unsent_frame_numbers([0, 1, 2, 3, 4], 5)
    assert result == [5]  # unsent_frame_numbers adds num_frames as extra


def test_unsent_frames_with_gaps():
    result = unsent_frame_numbers([0, 2, 4], 5)
    # gaps: [1, 3], extra: [5]
    assert 1 in result
    assert 3 in result


def test_unsent_frames_sequential():
    result = unsent_frame_numbers([0, 1, 2], 5)
    # gaps: [], extra: [3, 4, 5]
    assert 3 in result
    assert 4 in result
    assert 5 in result


# ---------------------------------------------------------------------------
# get_most_recent_run
# ---------------------------------------------------------------------------


def test_get_most_recent_run():
    mock_container = MagicMock()
    mock_container.keys.return_value = ["uid1", "uid2", "uid3"]
    mock_run = MagicMock()
    mock_container.__getitem__ = MagicMock(return_value=mock_run)

    result = get_most_recent_run(mock_container)
    mock_container.__getitem__.assert_called_once_with("uid3")
    assert result is mock_run


# ---------------------------------------------------------------------------
# sub_container
# ---------------------------------------------------------------------------


def test_sub_container():
    mock_run = MagicMock()
    child1 = MagicMock()
    child2 = MagicMock()
    mock_run.__getitem__ = MagicMock(return_value=child1)
    child1.__getitem__ = MagicMock(return_value=child2)

    result = sub_container(mock_run, ["primary", "data"])
    assert result is child2


# ---------------------------------------------------------------------------
# create_run_container
# ---------------------------------------------------------------------------


def test_create_run_container_new():
    client = MagicMock()
    client.__contains__ = MagicMock(return_value=False)
    new_container = MagicMock()
    client.create_container.return_value = new_container

    result = create_run_container(client, "new_run")
    client.create_container.assert_called_once_with("new_run")
    assert result is new_container


def test_create_run_container_existing():
    client = MagicMock()
    client.__contains__ = MagicMock(return_value=True)
    existing = MagicMock()
    client.__getitem__ = MagicMock(return_value=existing)

    result = create_run_container(client, "existing_run")
    client.create_container.assert_not_called()
    assert result is existing


# ---------------------------------------------------------------------------
# get_run_container
# ---------------------------------------------------------------------------


def test_get_run_container_creates_if_not_exists():
    runs_container = MagicMock()
    runs_container.__contains__ = MagicMock(return_value=False)
    new_run = MagicMock()
    runs_container.create_container.return_value = new_run

    start = SASStart(
        run_name="scan_42",
        run_id="uuid-abc",
        width=10,
        height=10,
        data_type="float32",
        tiled_url="http://example.com",
    )
    result = get_run_container(runs_container, start)
    runs_container.create_container.assert_called_once_with("scan_42_uuid-abc")
    assert result is new_run


def test_get_run_container_returns_existing():
    runs_container = MagicMock()
    runs_container.__contains__ = MagicMock(return_value=True)
    existing = MagicMock()
    runs_container.__getitem__ = MagicMock(return_value=existing)

    start = SASStart(
        run_name="scan_42",
        run_id="uuid-abc",
        width=10,
        height=10,
        data_type="float32",
        tiled_url="http://example.com",
    )
    result = get_run_container(runs_container, start)
    assert result is existing


# ---------------------------------------------------------------------------
# TiledRawFrameOperator
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tiled_raw_frame_operator_publishes():
    op = TiledRawFrameOperator()

    start = SASStart(
        run_name="run1",
        run_id="id1",
        width=10,
        height=10,
        data_type="float32",
        tiled_url="http://example.com",
    )
    published = []

    async def mock_publish(msg):
        published.append(msg)

    # Monkey-patch the publish method
    op.publish = mock_publish
    await op.process(start)
    assert len(published) == 1
    assert published[0] is start


# ---------------------------------------------------------------------------
# TiledPollingRedisListener
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redis_listener_processes_message():
    operator = AsyncMock()
    beamline_runs = MagicMock()
    beamline_runs.uri = "http://example.com/runs"

    # Create a mock array that can be read
    mock_array = MagicMock()
    mock_array.read.return_value = np.zeros((10, 10), dtype=np.float32)
    beamline_runs.__getitem__ = MagicMock(return_value=mock_array)

    file_watcher_msg = {
        "file_path": "http://example.com/runs/scan1/data.edf",
        "event_type": "created",
    }

    # Create a pubsub that yields one message
    class FakePubSub:
        async def subscribe(self, channel):
            pass

        async def listen(self):
            yield {"type": "subscribe", "data": 1}
            yield {"type": "message", "data": json.dumps(file_watcher_msg)}
            # Stop after one message
            raise asyncio.CancelledError()

    # redis_client must be a MagicMock so pubsub() returns a regular object
    redis_client = MagicMock()
    redis_client.pubsub.return_value = FakePubSub()

    listener = TiledPollingRedisListener(
        operator=operator,
        beamline_runs_tiled=beamline_runs,
        tiled_frame_segments=["primary", "data"],
        redis_client=redis_client,
        channel_name="sas_file_watcher",
    )

    with pytest.raises(asyncio.CancelledError):
        await listener.start()


# ---------------------------------------------------------------------------
# TiledProcessedPublisher
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_processed_publisher_handles_start():
    root_container = MagicMock()
    publisher = TiledProcessedPublisher(root_container)
    publisher._publishers = []

    run_container = MagicMock()

    start = SASStart(
        run_name="run1",
        run_id="id1",
        width=10,
        height=10,
        data_type="float32",
        tiled_url="http://example.com",
    )

    with patch("arroyosas.tiled.tiled_poller.get_run_container", return_value=run_container):
        await publisher.publish(start)

    assert publisher.run_node is run_container


@pytest.mark.asyncio
async def test_processed_publisher_handles_stop():
    root_container = MagicMock()
    publisher = TiledProcessedPublisher(root_container)
    publisher.run_node = MagicMock()

    stop = SASStop(num_frames=5)
    await publisher.publish(stop)  # Should not raise


@pytest.mark.asyncio
async def test_processed_publisher_no_run_node_logs_error():
    root_container = MagicMock()
    publisher = TiledProcessedPublisher(root_container)
    publisher.run_node = None

    # Publish a non-start, non-stop message without run_node
    from arroyosas.schemas import SAS1DReduction

    curve = SerializableNumpyArrayModel(array=np.array([1.0, 2.0]))
    raw = SerializableNumpyArrayModel(array=np.array([[1.0], [2.0]]))
    msg = SAS1DReduction(
        curve=curve,
        curve_tiled_url="http://c.com",
        raw_frame=raw,
        raw_frame_tiled_url="http://r.com",
    )
    await publisher.publish(msg)  # Should log error and return


@pytest.mark.asyncio
async def test_processed_publisher_handles_latent_space_event():
    # tiled_poller imports LatentSpaceEvent from arroyosas.schemas (not lse_reduction)
    from arroyosas.schemas import LatentSpaceEvent as LSEEvent

    root_container = MagicMock()
    publisher = TiledProcessedPublisher(root_container)
    publisher.run_node = MagicMock()

    event = LSEEvent(
        tiled_url="http://example.com",
        feature_vector=[0.1, 0.2],
        index=0,
    )

    mock_dim_node = MagicMock()
    with patch("arroyosas.tiled.tiled_poller.create_dim_reduction_node", return_value=mock_dim_node):
        await publisher.publish(event)

    assert publisher.dim_reduced_array_node is mock_dim_node
