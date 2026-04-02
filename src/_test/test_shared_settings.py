"""Tests for arroyosas.shared_settings"""

import json
from unittest.mock import patch

import fakeredis
import pytest

from arroyosas.shared_settings import KEY_CURRENT_REDUCTION_PARAMS, SharedSettings


@pytest.fixture
def fake_redis_instance():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def shared_settings(fake_redis_instance):
    # SharedSettings has a bug: __init__ ignores the passed redis arg and
    # calls redis.Redis(...) directly. We patch redis.Redis to return our
    # fakeredis instance.
    with patch("arroyosas.shared_settings.redis") as mock_redis_module:
        mock_redis_module.Redis.return_value = fake_redis_instance
        ss = SharedSettings(redis=mock_redis_module)
    return ss, fake_redis_instance


class TestSharedSettings:
    def test_init_creates_redis_client(self, fake_redis_instance):
        with patch("arroyosas.shared_settings.redis") as mock_redis_module:
            mock_redis_module.Redis.return_value = fake_redis_instance
            SharedSettings(redis=mock_redis_module)
            mock_redis_module.Redis.assert_called_once_with(host="localhost", port=6379, db=0)

    def test_set_and_get(self, shared_settings):
        ss, _ = shared_settings
        ss.set("mykey", "myvalue")
        result = ss.get("mykey")
        assert result == "myvalue"

    def test_get_missing_key_returns_none(self, shared_settings):
        ss, _ = shared_settings
        assert ss.get("nonexistent") is None

    def test_set_json_and_get_json(self, shared_settings):
        ss, _ = shared_settings
        data = {"foo": "bar", "num": 42}
        ss.set_json("jsonkey", data)
        result = ss.get_json("jsonkey")
        assert result == data

    def test_get_json_missing_key_returns_empty_dict(self, shared_settings):
        ss, _ = shared_settings
        result = ss.get_json("no_such_key")
        assert result == {}

    def test_get_json_parses_json_correctly(self, shared_settings):
        ss, redis_inst = shared_settings
        redis_inst.set("rawjson", json.dumps({"a": 1}))
        result = ss.get_json("rawjson")
        assert result == {"a": 1}

    def test_set_json_serializes_to_string(self, shared_settings):
        ss, redis_inst = shared_settings
        ss.set_json("jkey", {"x": [1, 2, 3]})
        raw = redis_inst.get("jkey")
        assert json.loads(raw) == {"x": [1, 2, 3]}

    def test_key_constant_defined(self):
        assert KEY_CURRENT_REDUCTION_PARAMS == "current_reduction_params"
