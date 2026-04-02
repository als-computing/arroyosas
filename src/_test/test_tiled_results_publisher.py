"""Tests for arroyosas.lse_reduction.tiled_results_publisher (TiledResultsPublisher)"""
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from arroyosas.lse_reduction.schemas import LatentSpaceEvent
from arroyosas.lse_reduction.tiled_results_publisher import TiledResultsPublisher
from arroyosas.schemas import SASStop

pytestmark = pytest.mark.asyncio


@pytest.fixture
def mock_container():
    """Create a mock container hierarchy."""
    day_container = MagicMock()
    day_container.__contains__ = MagicMock(return_value=False)
    day_container.__iter__ = MagicMock(return_value=iter([]))
    day_container.create_container.return_value = MagicMock()

    month_container = MagicMock()
    month_container.__contains__ = MagicMock(return_value=False)
    month_container.create_container.return_value = day_container
    month_container.__getitem__ = MagicMock(return_value=day_container)

    year_container = MagicMock()
    year_container.__contains__ = MagicMock(return_value=False)
    year_container.create_container.return_value = month_container
    year_container.__getitem__ = MagicMock(return_value=month_container)

    root_container = MagicMock()
    root_container.__contains__ = MagicMock(return_value=False)
    root_container.create_container.return_value = year_container
    root_container.__getitem__ = MagicMock(return_value=year_container)

    client = MagicMock()
    client.__contains__ = MagicMock(return_value=False)
    client.create_container.return_value = root_container
    client.__getitem__ = MagicMock(return_value=root_container)

    return client, root_container, year_container, month_container, day_container


@pytest.fixture
def publisher(mock_container):
    """Create a TiledResultsPublisher with mocked tiled client."""
    client, root, year, month, day = mock_container

    pub = TiledResultsPublisher(
        tiled_uri="http://tiled:8000",
        tiled_api_key="test_key",
        root_segments=["lse_results"],
        tiled_prefix=None,
    )
    # Manually set up the container state
    pub.client = client
    pub.root_container = root
    pub.year_container = year
    pub.month_container = month
    pub.day_container = day

    return pub, day


class TestTiledResultsPublisherInit:
    def test_init_defaults(self):
        pub = TiledResultsPublisher()
        assert pub.tiled_uri == "http://tiled:8000" or pub.tiled_uri is not None
        assert pub.root_segments == ["lse_live_results"]
        assert pub.current_experiment_name == "default_experiment"

    def test_init_custom(self):
        pub = TiledResultsPublisher(
            tiled_uri="http://custom:9000",
            tiled_api_key="mykey",
            root_segments=["custom", "path"],
            tiled_prefix="prefix",
        )
        assert pub.tiled_uri == "http://custom:9000"
        assert pub.root_segments == ["custom", "path"]
        assert pub.tiled_prefix == "prefix"


class TestExtractUuidFromUrl:
    def test_extracts_valid_uuid(self):
        pub = TiledResultsPublisher()
        url = "http://tiled.example.com/api/v1/array/full/abc12345-1234-1234-1234-abcdef012345/primary/data/img"
        result = pub._extract_uuid_from_url(url)
        assert result == "abc12345-1234-1234-1234-abcdef012345"

    def test_returns_default_for_no_uuid(self):
        pub = TiledResultsPublisher()
        url = "http://example.com/no_uuid_here"
        result = pub._extract_uuid_from_url(url)
        assert result == pub.default_table_name

    def test_returns_default_for_empty_url(self):
        pub = TiledResultsPublisher()
        result = pub._extract_uuid_from_url(None)
        assert result == pub.default_table_name

    def test_returns_default_for_empty_string(self):
        pub = TiledResultsPublisher()
        result = pub._extract_uuid_from_url("")
        assert result == pub.default_table_name


class TestPublishSync:
    def test_publish_sync_with_valid_event(self, publisher):
        pub, day_container = publisher
        # Setup experiment container
        exp_container = MagicMock()
        exp_container.__contains__ = MagicMock(return_value=False)
        day_container.create_container.return_value = exp_container
        day_container.__contains__ = MagicMock(return_value=False)
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        event = LatentSpaceEvent(
            tiled_url="http://tiled.example.com/run/abc12345-1234-1234-1234-abcdef012345/data",
            feature_vector=[0.1, 0.2, 0.3],
            index=0,
            experiment_name="exp_001",
        )
        result = pub._publish_sync(event)
        # First event - no previous UUID to write
        assert result is None

    def test_publish_sync_no_day_container(self, publisher):
        pub, _ = publisher
        pub.day_container = None

        event = LatentSpaceEvent(
            tiled_url="http://example.com",
            feature_vector=[1.0],
            index=0,
        )
        result = pub._publish_sync(event)
        assert result is None

    def test_publish_sync_new_uuid_triggers_write(self, publisher):
        pub, day_container = publisher
        exp_container = MagicMock()
        exp_container.__contains__ = MagicMock(return_value=False)
        exp_container.get = MagicMock(return_value=None)
        day_container.__contains__ = MagicMock(return_value=False)
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        uuid1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        uuid2 = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

        event1 = LatentSpaceEvent(
            tiled_url=f"http://tiled.example.com/{uuid1}/data",
            feature_vector=[0.1],
            index=0,
        )
        event2 = LatentSpaceEvent(
            tiled_url=f"http://tiled.example.com/{uuid2}/data",
            feature_vector=[0.2],
            index=1,
        )

        # Publish first event
        pub._publish_sync(event1)
        # Publish second with different UUID - should trigger write of first
        result = pub._publish_sync(event2)
        # uuid1 should be returned for writing
        assert result == uuid1


class TestPublishAsync:
    async def test_publish_flush_signal(self, publisher):
        pub, _ = publisher
        pub.current_uuid = "some-uuid"
        pub.uuid_dataframes = {"some-uuid": pd.DataFrame([{"x": 1}])}

        with patch.object(pub, "write_table_to_tiled", new=AsyncMock()) as mock_write:
            flush_event = LatentSpaceEvent(
                tiled_url="FLUSH_SIGNAL",
                feature_vector=[],
                index=-1,
            )
            await pub.publish(flush_event)
            mock_write.assert_called_once_with("some-uuid")

    async def test_publish_sas_stop_calls_stop(self, publisher):
        pub, _ = publisher
        with patch.object(pub, "stop", new=AsyncMock()) as mock_stop:
            await pub.publish(SASStop(num_frames=5))
            mock_stop.assert_called_once()

    async def test_publish_non_event_ignored(self, publisher):
        pub, _ = publisher
        from arroyosas.schemas import SASStart

        start = SASStart(
            run_name="run1",
            run_id="id1",
            width=10,
            height=10,
            data_type="float32",
            tiled_url="http://example.com",
        )
        # Should not raise
        await pub.publish(start)

    async def test_publish_latent_space_event(self, publisher):
        pub, day_container = publisher
        exp_container = MagicMock()
        exp_container.__contains__ = MagicMock(return_value=False)
        exp_container.get = MagicMock(return_value=None)
        day_container.__contains__ = MagicMock(return_value=False)
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        event = LatentSpaceEvent(
            tiled_url="http://tiled.example.com/run/aabbccdd-0000-0000-0000-000000000001/data",
            feature_vector=[0.1, 0.2],
            index=0,
            experiment_name="test_exp",
        )
        with patch.object(pub, "write_table_to_tiled", new=AsyncMock()):
            await pub.publish(event)
        # Should have added to uuid_dataframes
        assert len(pub.uuid_dataframes) > 0


class TestStopSync:
    def test_stop_sync_with_pending_data(self, publisher):
        pub, day_container = publisher
        uuid = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        pub.current_uuid = uuid
        pub.uuid_dataframes = {uuid: pd.DataFrame([{"x": 1}])}

        exp_container = MagicMock()
        exp_container.__contains__ = MagicMock(return_value=False)
        day_container.__contains__ = MagicMock(return_value=False)
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        result = pub._stop_sync()
        assert result == uuid

    def test_stop_sync_no_pending_data(self, publisher):
        pub, _ = publisher
        pub.current_uuid = None
        result = pub._stop_sync()
        assert result is None


class TestStartSync:
    async def test_start_calls_start_sync(self):
        pub = TiledResultsPublisher()

        with patch.object(pub, "_start_sync") as mock_start:
            await pub.start()
            mock_start.assert_called_once()

    def test_start_sync_with_prefix(self):
        pub = TiledResultsPublisher(tiled_prefix="my/prefix")
        mock_client = MagicMock()
        prefix_container = MagicMock()
        prefix_container.__contains__ = MagicMock(return_value=True)
        prefix_container.__getitem__ = MagicMock(return_value=prefix_container)

        with patch("arroyosas.lse_reduction.tiled_results_publisher.from_uri", return_value=mock_client), patch.object(
            pub, "_setup_containers_sync"
        ) as mock_setup:
            mock_client.__contains__ = MagicMock(return_value=True)
            mock_client.__getitem__ = MagicMock(return_value=prefix_container)
            pub.day_container = MagicMock()
            pub.day_container.__iter__ = MagicMock(return_value=iter([]))
            pub._start_sync()
            mock_setup.assert_called_once()
