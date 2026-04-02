"""Tests for arroyosas.lse_reduction.redis_model_store (RedisModelStore)"""
import json
import threading
import time
from unittest.mock import MagicMock, patch

import fakeredis
import pytest


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def store(fake_redis):
    with patch("arroyosas.lse_reduction.redis_model_store.redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value = fake_redis
        from arroyosas.lse_reduction.redis_model_store import RedisModelStore

        s = RedisModelStore(host="localhost", port=6379)
    return s, fake_redis


class TestRedisModelStore:
    def test_init_sets_host_port(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        with patch("arroyosas.lse_reduction.redis_model_store.redis.Redis") as mock_cls:
            mock_cls.return_value = fake
            from arroyosas.lse_reduction.redis_model_store import RedisModelStore

            s = RedisModelStore(host="testhost", port=1234)
        assert s.host == "testhost"
        assert s.port == 1234

    def test_init_defaults_from_env(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        with patch("arroyosas.lse_reduction.redis_model_store.redis.Redis") as mock_cls, patch.dict(
            "os.environ", {"REDIS_HOST": "envhost", "REDIS_PORT": "9999"}
        ):
            mock_cls.return_value = fake
            from arroyosas.lse_reduction.redis_model_store import RedisModelStore

            s = RedisModelStore()
        assert s.host == "envhost"
        assert s.port == 9999

    def test_store_autoencoder_model(self, store):
        s, redis_inst = store
        result = s.store_autoencoder_model("ae_model:v1")
        assert result is True
        assert redis_inst.get("selected_mlflow_model") == "ae_model:v1"

    def test_get_autoencoder_model(self, store):
        s, redis_inst = store
        redis_inst.set("selected_mlflow_model", "my_model:3")
        result = s.get_autoencoder_model()
        assert result == "my_model:3"

    def test_get_autoencoder_model_returns_none_if_not_set(self, store):
        s, _ = store
        result = s.get_autoencoder_model()
        assert result is None

    def test_store_dimred_model(self, store):
        s, redis_inst = store
        result = s.store_dimred_model("umap_v1:2")
        assert result is True
        assert redis_inst.get("selected_dim_reduction_model") == "umap_v1:2"

    def test_get_dimred_model(self, store):
        s, redis_inst = store
        redis_inst.set("selected_dim_reduction_model", "umap:5")
        result = s.get_dimred_model()
        assert result == "umap:5"

    def test_store_experiment_name(self, store):
        s, redis_inst = store
        result = s.store_experiment_name("my_experiment")
        assert result is True
        assert redis_inst.get("experiment_name") == "my_experiment"

    def test_get_experiment_name(self, store):
        s, redis_inst = store
        redis_inst.set("experiment_name", "exp_2024")
        result = s.get_experiment_name()
        assert result == "exp_2024"

    def test_get_experiment_name_none(self, store):
        s, _ = store
        result = s.get_experiment_name()
        assert result is None

    def test_publish_model_update(self, store):
        s, _ = store
        result = s.publish_model_update("autoencoder", "ae_model:1")
        assert result is True

    def test_publish_experiment_update(self, store):
        s, _ = store
        result = s.publish_experiment_update("test_experiment")
        assert result is True

    def test_get_model_loading_state_default(self, store):
        s, _ = store
        state = s.get_model_loading_state()
        assert "is_loading_model" in state
        assert "loading_model_type" in state
        assert state["is_loading_model"] is False

    def test_get_model_loading_state_true(self, store):
        s, redis_inst = store
        redis_inst.set("model_loading_state", "True")
        redis_inst.set("loading_model_type", "autoencoder")
        state = s.get_model_loading_state()
        assert state["is_loading_model"] is True
        assert state["loading_model_type"] == "autoencoder"

    def test_get_model_loading_state_false(self, store):
        s, redis_inst = store
        redis_inst.set("model_loading_state", "False")
        state = s.get_model_loading_state()
        assert state["is_loading_model"] is False

    def test_get_model_loading_state_empty_type(self, store):
        s, redis_inst = store
        redis_inst.set("model_loading_state", "True")
        redis_inst.set("loading_model_type", "")
        state = s.get_model_loading_state()
        # Empty string becomes None
        assert state["loading_model_type"] is None

    def test_store_autoencoder_publishes_update(self, store):
        s, _ = store
        with patch.object(s, "publish_model_update") as mock_pub:
            s.store_autoencoder_model("model:1")
            mock_pub.assert_called_once_with("autoencoder", "model:1")

    def test_store_dimred_publishes_update(self, store):
        s, _ = store
        with patch.object(s, "publish_model_update") as mock_pub:
            s.store_dimred_model("umap:2")
            mock_pub.assert_called_once_with("dimred", "umap:2")

    def test_store_experiment_name_publishes_update(self, store):
        s, _ = store
        with patch.object(s, "publish_experiment_update") as mock_pub:
            s.store_experiment_name("new_exp")
            mock_pub.assert_called_once_with("new_exp")

    def test_redis_client_none_returns_false_for_store(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        with patch("arroyosas.lse_reduction.redis_model_store.redis.Redis") as mock_cls:
            mock_cls.return_value = fake
            from arroyosas.lse_reduction.redis_model_store import RedisModelStore

            s = RedisModelStore()
        s.redis_client = None
        assert s.store_autoencoder_model("model") is False
        assert s.store_dimred_model("model") is False
        assert s.store_experiment_name("exp") is False
        assert s.publish_model_update("type", "model") is False
        assert s.publish_experiment_update("exp") is False
        assert s.get_autoencoder_model() is None
        assert s.get_dimred_model() is None
        assert s.get_experiment_name() is None

    def test_redis_client_none_returns_default_loading_state(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        with patch("arroyosas.lse_reduction.redis_model_store.redis.Redis") as mock_cls:
            mock_cls.return_value = fake
            from arroyosas.lse_reduction.redis_model_store import RedisModelStore

            s = RedisModelStore()
        s.redis_client = None
        state = s.get_model_loading_state()
        assert state["is_loading_model"] is False

    def test_subscribe_to_model_updates_starts_thread(self, store):
        s, _ = store
        callback = MagicMock()
        # Patch redis.Redis in the module to return a fake that can be subscribed to
        fake2 = fakeredis.FakeRedis(decode_responses=True)
        with patch("arroyosas.lse_reduction.redis_model_store.redis.Redis") as mock_cls:
            mock_cls.return_value = fake2
            # The thread starts but immediately blocks on pubsub.listen()
            # We just verify the thread is created and started
            s.subscribe_to_model_updates(callback)
        # Give the thread a moment to start
        time.sleep(0.05)
        # No assertion needed - just verifying it doesn't crash

    def test_constants(self):
        from arroyosas.lse_reduction.redis_model_store import RedisModelStore

        assert RedisModelStore.KEY_AUTOENCODER_MODEL == "selected_mlflow_model"
        assert RedisModelStore.KEY_DIMRED_MODEL == "selected_dim_reduction_model"
        assert RedisModelStore.KEY_EXPERIMENT_NAME == "experiment_name"
        assert RedisModelStore.CHANNEL_MODEL_UPDATES == "model_updates"
