"""Additional tests for arroyosas.lse_reduction.tiled_results_publisher."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from arroyosas.lse_reduction.tiled_results_publisher import (
    TiledResultsPublisher,
    create_tiled_results_publisher,
)


def _make_publisher_with_containers(
    has_day_container=True,
    tiled_prefix=None,
):
    """Helper to create a publisher with pre-set containers."""
    pub = TiledResultsPublisher(tiled_prefix=tiled_prefix)

    if has_day_container:
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

        pub.client = MagicMock()
        pub.root_container = root_container
        pub.year_container = year_container
        pub.month_container = month_container
        pub.day_container = day_container
        return pub, day_container

    return pub, None


# ---------------------------------------------------------------------------
# _setup_containers_sync
# ---------------------------------------------------------------------------


class TestSetupContainersSync:
    def test_setup_creates_year_month_day(self):
        """Test that _setup_containers_sync creates year/month/day containers."""
        pub = TiledResultsPublisher()

        day = MagicMock()
        month = MagicMock()
        month.__contains__ = MagicMock(return_value=False)
        month.create_container.return_value = day
        month.__getitem__ = MagicMock(return_value=day)

        year = MagicMock()
        year.__contains__ = MagicMock(return_value=False)
        year.create_container.return_value = month
        year.__getitem__ = MagicMock(return_value=month)

        root = MagicMock()
        root.__contains__ = MagicMock(side_effect=lambda k: False)
        root.create_container.return_value = year
        root.__getitem__ = MagicMock(return_value=year)

        starting = MagicMock()
        starting.__contains__ = MagicMock(return_value=False)
        starting.create_container.return_value = root
        starting.__getitem__ = MagicMock(return_value=root)

        pub._setup_containers_sync(starting_container=starting)

        assert pub.day_container is day
        assert pub.month_container is month
        assert pub.year_container is year

    def test_setup_uses_existing_containers(self):
        """Test that _setup_containers_sync uses existing containers."""
        pub = TiledResultsPublisher(root_segments=["existing_root"])

        day = MagicMock()
        month = MagicMock()
        month.__contains__ = MagicMock(return_value=True)
        month.__getitem__ = MagicMock(return_value=day)

        year = MagicMock()
        year.__contains__ = MagicMock(return_value=True)
        year.__getitem__ = MagicMock(return_value=month)

        root = MagicMock()
        root.__contains__ = MagicMock(return_value=True)
        root.__getitem__ = MagicMock(return_value=year)

        starting = MagicMock()
        starting.__contains__ = MagicMock(return_value=True)
        starting.__getitem__ = MagicMock(return_value=root)

        pub._setup_containers_sync(starting_container=starting)

        assert pub.day_container is day
        # create_container should not have been called since everything exists
        root.create_container.assert_not_called()

    def test_setup_exception_reraises(self):
        """Test that exceptions in _setup_containers_sync are reraised."""
        pub = TiledResultsPublisher()

        starting = MagicMock()
        starting.__contains__ = MagicMock(side_effect=Exception("container error"))

        with pytest.raises(Exception, match="container error"):
            pub._setup_containers_sync(starting_container=starting)


# ---------------------------------------------------------------------------
# _write_table_to_tiled_sync
# ---------------------------------------------------------------------------


class TestWriteTableToTiledSync:
    def test_write_table_skips_existing_feature_vectors(self):
        """Test that write is skipped if feature_vectors already exists."""
        pub, day_container = _make_publisher_with_containers()

        uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        pub.current_uuid = uuid
        pub.current_experiment_name = "test_exp"

        # Setup: uuid container already has feature_vectors
        uuid_container = MagicMock()
        uuid_container.__contains__ = MagicMock(return_value=True)  # "feature_vectors" in uuid_container

        exp_container = MagicMock()
        exp_container.__contains__ = MagicMock(return_value=True)  # uuid in exp_container
        exp_container.__getitem__ = MagicMock(return_value=uuid_container)
        day_container.__contains__ = MagicMock(return_value=False)
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        pub._write_table_to_tiled_sync(uuid)
        # write_dataframe should NOT have been called
        uuid_container.write_dataframe.assert_not_called()

    def test_write_table_skips_when_no_dataframe(self):
        """Test that write is skipped when no DataFrame for the key."""
        pub, day_container = _make_publisher_with_containers()

        pub.current_experiment_name = "test_exp"
        exp_container = MagicMock()
        exp_container.__contains__ = MagicMock(return_value=False)
        day_container.__contains__ = MagicMock(return_value=False)
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        # No dataframe stored
        pub.uuid_dataframes = {}

        pub._write_table_to_tiled_sync("nonexistent-uuid")
        # write_dataframe should NOT have been called
        exp_container.write_dataframe.assert_not_called()

    def test_write_table_skips_empty_dataframe(self):
        """Test that write is skipped when DataFrame is empty."""
        pub, day_container = _make_publisher_with_containers()

        uuid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        pub.current_uuid = uuid
        pub.current_experiment_name = "test_exp"

        exp_container = MagicMock()
        exp_container.__contains__ = MagicMock(return_value=False)
        day_container.__contains__ = MagicMock(return_value=False)
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        pub.uuid_dataframes = {uuid: pd.DataFrame()}  # empty

        pub._write_table_to_tiled_sync(uuid)
        # write_dataframe should NOT be called for empty DF
        exp_container.write_dataframe.assert_not_called()

    def test_write_table_creates_uuid_container_and_writes(self):
        """Test the happy path: creates UUID container and writes DataFrame."""
        pub, day_container = _make_publisher_with_containers()

        uuid = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        pub.current_uuid = uuid
        pub.current_experiment_name = "exp_1"

        uuid_container = MagicMock()
        uuid_container.write_dataframe = MagicMock()

        exp_container = MagicMock()
        exp_container.__contains__ = MagicMock(return_value=False)  # uuid not in exp_container
        exp_container.create_container = MagicMock(return_value=None)
        exp_container.__getitem__ = MagicMock(return_value=uuid_container)
        day_container.__contains__ = MagicMock(return_value=False)
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        df = pd.DataFrame([{"feature_0": 0.1, "feature_1": 0.2}])
        pub.uuid_dataframes = {uuid: df}

        pub._write_table_to_tiled_sync(uuid)

        exp_container.create_container.assert_called_once_with(uuid)
        uuid_container.write_dataframe.assert_called_once()
        # uuid should be in existing_uuids
        assert uuid in pub.existing_uuids
        # DataFrame should be cleared
        assert pub.uuid_dataframes[uuid].empty

    def test_write_table_exception_handled(self):
        """Test that exceptions in _write_table_to_tiled_sync are handled."""
        pub, day_container = _make_publisher_with_containers()

        pub.current_experiment_name = "exp_1"
        day_container.__contains__ = MagicMock(side_effect=Exception("container error"))

        # Should not raise
        pub._write_table_to_tiled_sync("some-uuid")


# ---------------------------------------------------------------------------
# write_table_to_tiled (async wrapper)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_table_to_tiled_calls_sync():
    pub = TiledResultsPublisher()
    with patch.object(pub, "_write_table_to_tiled_sync") as mock_sync:
        await pub.write_table_to_tiled("some-uuid")
        mock_sync.assert_called_once_with("some-uuid")


@pytest.mark.asyncio
async def test_write_table_to_tiled_handles_exception():
    pub = TiledResultsPublisher()
    with patch.object(pub, "_write_table_to_tiled_sync", side_effect=Exception("write error")):
        # Should not raise
        await pub.write_table_to_tiled("some-uuid")


# ---------------------------------------------------------------------------
# _get_experiment_container
# ---------------------------------------------------------------------------


class TestGetExperimentContainer:
    def test_creates_experiment_container_if_missing(self):
        pub, day_container = _make_publisher_with_containers()
        pub.current_experiment_name = "exp_test"

        exp_container = MagicMock()
        day_container.__contains__ = MagicMock(return_value=False)
        day_container.create_container = MagicMock()
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        result = pub._get_experiment_container("exp_test")
        day_container.create_container.assert_called_once_with("exp_test")
        assert result is exp_container

    def test_uses_existing_experiment_container(self):
        pub, day_container = _make_publisher_with_containers()
        pub.current_experiment_name = "exp_test"

        exp_container = MagicMock()
        day_container.__contains__ = MagicMock(return_value=True)
        day_container.__getitem__ = MagicMock(return_value=exp_container)

        result = pub._get_experiment_container("exp_test")
        day_container.create_container.assert_not_called()
        assert result is exp_container

    def test_falls_back_to_day_container_on_error(self):
        pub, day_container = _make_publisher_with_containers()
        pub.current_experiment_name = "exp_test"

        day_container.__contains__ = MagicMock(side_effect=Exception("error"))

        result = pub._get_experiment_container("exp_test")
        assert result is day_container


# ---------------------------------------------------------------------------
# _stop_sync - uuid container has feature_vectors
# ---------------------------------------------------------------------------


def test_stop_sync_uuid_already_written():
    """Test that _stop_sync returns None if feature_vectors already exists."""
    pub, day_container = _make_publisher_with_containers()
    uuid = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    pub.current_uuid = uuid
    pub.uuid_dataframes = {uuid: pd.DataFrame([{"x": 1}])}
    pub.current_experiment_name = "exp_1"

    uuid_container = MagicMock()
    uuid_container.__contains__ = MagicMock(return_value=True)  # feature_vectors exists

    exp_container = MagicMock()
    exp_container.__contains__ = MagicMock(return_value=True)  # uuid in exp_container
    exp_container.__getitem__ = MagicMock(return_value=uuid_container)
    day_container.__contains__ = MagicMock(return_value=False)
    day_container.__getitem__ = MagicMock(return_value=exp_container)

    result = pub._stop_sync()
    assert result is None


def test_stop_sync_exception():
    """Test that _stop_sync handles exceptions."""
    pub, day_container = _make_publisher_with_containers()
    pub.current_uuid = "some-uuid"
    pub.uuid_dataframes = {"some-uuid": pd.DataFrame([{"x": 1}])}
    pub.current_experiment_name = "exp_1"

    day_container.__contains__ = MagicMock(side_effect=Exception("container error"))

    result = pub._stop_sync()
    assert result is None


# ---------------------------------------------------------------------------
# _start_sync - no prefix path
# ---------------------------------------------------------------------------


def test_start_sync_no_prefix():
    """Test _start_sync without tiled_prefix."""
    pub = TiledResultsPublisher()

    mock_client = MagicMock()

    with (
        patch("arroyosas.lse_reduction.tiled_results_publisher.from_uri", return_value=mock_client),
        patch.object(pub, "_setup_containers_sync") as mock_setup,
    ):
        pub.day_container = MagicMock()
        pub.day_container.__iter__ = MagicMock(return_value=iter(["uuid1", "uuid2"]))
        pub._start_sync()
        mock_setup.assert_called_once_with(mock_client)


def test_start_sync_creates_prefix_container():
    """Test _start_sync creates prefix containers that don't exist."""
    pub = TiledResultsPublisher(tiled_prefix="my/prefix")

    mock_client = MagicMock()
    prefix_node = MagicMock()
    prefix_node.__contains__ = MagicMock(return_value=False)
    prefix_node.create_container = MagicMock(return_value=prefix_node)
    prefix_node.__getitem__ = MagicMock(return_value=prefix_node)

    mock_client.__contains__ = MagicMock(return_value=False)
    mock_client.create_container = MagicMock(return_value=prefix_node)
    mock_client.__getitem__ = MagicMock(return_value=prefix_node)

    with (
        patch("arroyosas.lse_reduction.tiled_results_publisher.from_uri", return_value=mock_client),
        patch.object(pub, "_setup_containers_sync") as mock_setup,
    ):
        pub.day_container = MagicMock()
        pub.day_container.__iter__ = MagicMock(return_value=iter([]))
        pub._start_sync()
        mock_setup.assert_called_once()


def test_start_sync_exception_reraises():
    """Test that _start_sync exceptions are logged and reraised."""
    pub = TiledResultsPublisher()

    with patch(
        "arroyosas.lse_reduction.tiled_results_publisher.from_uri",
        side_effect=Exception("connection error"),
    ):
        with pytest.raises(Exception, match="connection error"):
            pub._start_sync()


# ---------------------------------------------------------------------------
# create_tiled_results_publisher
# ---------------------------------------------------------------------------


def test_create_tiled_results_publisher():
    """Test the factory function creates a TiledResultsPublisher."""
    pub = create_tiled_results_publisher(
        tiled_uri="http://tiled:8000",
        tiled_api_key="key",
        root_segments=["results"],
        tiled_prefix="prefix",
    )
    assert isinstance(pub, TiledResultsPublisher)
    assert pub.tiled_prefix == "prefix"
