"""Tests for arroyosas.lse_reduction.mlflow_utils (MLflowClient)"""
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def clear_mlflow_cache():
    """Clear the in-memory model cache before each test."""
    from arroyosas.lse_reduction.mlflow_utils import MLflowClient

    MLflowClient._model_cache.clear()
    yield
    MLflowClient._model_cache.clear()


@pytest.fixture
def mock_mlflow():
    """Patch all mlflow calls used by MLflowClient."""
    with patch("arroyosas.lse_reduction.mlflow_utils.mlflow") as mock_mlflow, patch(
        "arroyosas.lse_reduction.mlflow_utils.MlflowClient"
    ) as mock_mlflow_client_cls:
        mock_inner_client = MagicMock()
        mock_mlflow_client_cls.return_value = mock_inner_client
        yield mock_mlflow, mock_mlflow_client_cls, mock_inner_client


@pytest.fixture
def client(mock_mlflow, tmp_path):
    _, _, _ = mock_mlflow
    from arroyosas.lse_reduction.mlflow_utils import MLflowClient

    c = MLflowClient(
        tracking_uri="http://mlflow:5000",
        username="user",
        password="pass",
        cache_dir=str(tmp_path / "mlflow_cache"),
    )
    return c


class TestMLflowClientInit:
    def test_init_sets_tracking_uri(self, mock_mlflow, tmp_path):
        mock_ml, _, _ = mock_mlflow
        from arroyosas.lse_reduction.mlflow_utils import MLflowClient

        c = MLflowClient(
            tracking_uri="http://mlflow:5000",
            cache_dir=str(tmp_path / "cache"),
        )
        mock_ml.set_tracking_uri.assert_called_once_with("http://mlflow:5000")

    def test_init_creates_cache_dir(self, mock_mlflow, tmp_path):
        _, _, _ = mock_mlflow
        from arroyosas.lse_reduction.mlflow_utils import MLflowClient

        cache_dir = str(tmp_path / "my_cache")
        c = MLflowClient(cache_dir=cache_dir)
        assert os.path.exists(cache_dir)

    def test_init_creates_inner_client(self, mock_mlflow, tmp_path):
        _, mock_cls, mock_inner = mock_mlflow
        from arroyosas.lse_reduction.mlflow_utils import MLflowClient

        c = MLflowClient(cache_dir=str(tmp_path / "cache"))
        mock_cls.assert_called_once()
        assert c.client is mock_inner


class TestCheckMlflowReady:
    def test_returns_true_when_server_reachable(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        inner.search_experiments.return_value = [MagicMock()]
        result = client.check_mlflow_ready()
        assert result is True

    def test_returns_false_when_server_unreachable(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        inner.search_experiments.side_effect = Exception("Connection refused")
        result = client.check_mlflow_ready()
        assert result is False


class TestGetMlflowParams:
    def test_returns_params_with_explicit_version(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        mock_version = MagicMock()
        mock_version.run_id = "run-123"
        inner.get_model_version.return_value = mock_version

        mock_run = MagicMock()
        mock_run.data.params = {"latent_dim": "64"}
        inner.get_run.return_value = mock_run

        params = client.get_mlflow_params("my_model", version="3")
        assert params["latent_dim"] == "64"
        inner.get_model_version.assert_called_once_with(name="my_model", version="3")

    def test_parses_name_version_format(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        mock_version = MagicMock()
        mock_version.run_id = "run-456"
        inner.get_model_version.return_value = mock_version
        mock_run = MagicMock()
        mock_run.data.params = {"input_dim": "32"}
        inner.get_run.return_value = mock_run

        params = client.get_mlflow_params("my_model:5")
        inner.get_model_version.assert_called_once_with(name="my_model", version="5")

    def test_defaults_to_version_1(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        mock_version = MagicMock()
        mock_version.run_id = "run-789"
        inner.get_model_version.return_value = mock_version
        mock_run = MagicMock()
        mock_run.data.params = {}
        inner.get_run.return_value = mock_run

        client.get_mlflow_params("simple_model")
        inner.get_model_version.assert_called_once_with(name="simple_model", version="1")


class TestGetModelVersions:
    def test_returns_sorted_versions(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        v1 = MagicMock()
        v1.version = "1"
        v3 = MagicMock()
        v3.version = "3"
        v2 = MagicMock()
        v2.version = "2"
        inner.search_model_versions.return_value = [v1, v3, v2]

        result = client.get_model_versions("my_model")
        assert result[0]["value"] == "3"  # Latest first
        assert result[1]["value"] == "2"
        assert result[2]["value"] == "1"

    def test_returns_empty_list_when_no_versions(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        inner.search_model_versions.return_value = []
        result = client.get_model_versions("nonexistent")
        assert result == []

    def test_returns_empty_on_exception(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        inner.search_model_versions.side_effect = Exception("MLflow error")
        result = client.get_model_versions("my_model")
        assert result == []


class TestLoadModel:
    def test_returns_none_for_none_model_name(self, client):
        result = client.load_model(None)
        assert result is None

    def test_uses_in_memory_cache(self, client, mock_mlflow):
        mock_model = MagicMock()
        from arroyosas.lse_reduction.mlflow_utils import MLflowClient

        MLflowClient._model_cache["cached_model:1"] = mock_model

        result = client.load_model("cached_model", version="1")
        assert result is mock_model

    def test_loads_from_mlflow_and_caches(self, client, mock_mlflow, tmp_path):
        mock_ml, _, inner = mock_mlflow
        mock_model = MagicMock()

        # No disk cache exists, so it downloads
        inner.search_model_versions.return_value = []  # Will fail to find version
        # Simulate version specified
        mock_ml.artifacts.download_artifacts.return_value = str(tmp_path / "model_cache")
        mock_ml.pyfunc.load_model.return_value = mock_model

        result = client.load_model("my_model", version="2")
        # Model should be in cache
        assert "my_model:2" in client._model_cache or result is not None

    def test_fallback_when_download_fails(self, client, mock_mlflow, tmp_path):
        mock_ml, _, inner = mock_mlflow
        mock_model = MagicMock()

        mock_ml.artifacts.download_artifacts.side_effect = Exception("Download failed")
        mock_ml.pyfunc.load_model.return_value = mock_model

        result = client.load_model("my_model", version="3")
        # Should fall back to direct loading
        assert result is mock_model

    def test_returns_none_on_total_failure(self, client, mock_mlflow, tmp_path):
        mock_ml, _, inner = mock_mlflow

        inner.search_model_versions.return_value = []
        mock_ml.artifacts.download_artifacts.side_effect = Exception("Download failed")
        mock_ml.pyfunc.load_model.side_effect = Exception("Load failed")

        result = client.load_model("broken_model", version="1")
        assert result is None

    def test_loads_from_disk_cache(self, client, mock_mlflow, tmp_path):
        mock_ml, _, inner = mock_mlflow
        mock_model = MagicMock()
        mock_ml.pyfunc.load_model.return_value = mock_model

        # Create a fake disk cache
        cache_path = client._get_cache_path("disk_model", "4")
        os.makedirs(cache_path, exist_ok=True)

        result = client.load_model("disk_model", version="4")
        assert result is mock_model
        mock_ml.pyfunc.load_model.assert_called_once_with(cache_path)


class TestCacheManagement:
    def test_clear_memory_cache(self, client, mock_mlflow):
        from arroyosas.lse_reduction.mlflow_utils import MLflowClient

        MLflowClient._model_cache["key"] = MagicMock()
        MLflowClient.clear_memory_cache()
        assert len(MLflowClient._model_cache) == 0

    def test_clear_disk_cache(self, client, mock_mlflow, tmp_path):
        # Create some files in cache dir
        cache_file = tmp_path / "mlflow_cache" / "test_file.txt"
        cache_file.parent.mkdir(exist_ok=True)
        cache_file.write_text("test")

        client.clear_disk_cache()
        assert os.path.exists(client.cache_dir)
        assert not cache_file.exists()

    def test_get_cache_path_with_version(self, client):
        path = client._get_cache_path("model_name", "5")
        assert "model_name_v5" in path

    def test_get_cache_path_without_version(self, client):
        path = client._get_cache_path("model_name")
        assert "model_name" in path


class TestCheckModelCompatibility:
    def test_compatible_models(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        # Mock get_mlflow_params for autoencoder
        mv1 = MagicMock()
        mv1.run_id = "run1"
        inner.get_model_version.side_effect = [mv1, mv1]
        run1 = MagicMock()
        run1.data.params = {"latent_dim": "32"}
        run2 = MagicMock()
        run2.data.params = {"input_dim": "32"}
        inner.get_run.side_effect = [run1, run2]

        result = client.check_model_compatibility("ae_model:1", "umap_model:1")
        assert result is True

    def test_incompatible_models(self, client, mock_mlflow):
        _, _, inner = mock_mlflow
        mv1 = MagicMock()
        mv1.run_id = "run1"
        inner.get_model_version.side_effect = [mv1, mv1]
        run1 = MagicMock()
        run1.data.params = {"latent_dim": "32"}
        run2 = MagicMock()
        run2.data.params = {"input_dim": "64"}
        inner.get_run.side_effect = [run1, run2]

        result = client.check_model_compatibility("ae_model:1", "umap_model:1")
        assert result is False

    def test_none_model_returns_false(self, client):
        assert client.check_model_compatibility(None, "model") is False
        assert client.check_model_compatibility("model", None) is False
        assert client.check_model_compatibility(None, None) is False
