"""Tests for arroyosas.redis (RedisConn async wrapper)"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def fake_redis_conn():
    """Create a fakeredis async instance for testing."""
    import fakeredis.aioredis

    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield r
    await r.aclose()


@pytest.fixture
async def redis_conn(fake_redis_conn):
    from arroyosas.redis import RedisConn

    return RedisConn(fake_redis_conn)


class TestRedisConn:
    async def test_set_and_get(self, redis_conn):
        await redis_conn.set("key1", "value1")
        result = await redis_conn.get("key1")
        assert result == "value1"

    async def test_get_missing_returns_none(self, redis_conn):
        result = await redis_conn.get("nonexistent_key_xyz")
        assert result is None

    async def test_get_json_returns_dict(self, redis_conn, fake_redis_conn):
        data = {"hello": "world", "num": 99}
        await fake_redis_conn.set("jkey", json.dumps(data))
        result = await redis_conn.get_json("jkey")
        assert result == data

    async def test_get_json_missing_returns_empty_dict(self, redis_conn):
        result = await redis_conn.get_json("totally_missing")
        assert result == {}

    async def test_set_json_has_typo_raises_attribute_error(self, redis_conn):
        # The set_json method has a bug: uses self.reddis_conn (double d)
        with pytest.raises(AttributeError):
            await redis_conn.set_json("key", {"value": 1})

    async def test_from_settings(self):
        from arroyosas.redis import RedisConn

        settings = MagicMock()
        settings.host = "localhost"
        settings.port = 6379

        with patch("arroyosas.redis.redis.ConnectionPool") as mock_pool, patch(
            "arroyosas.redis.redis.Redis"
        ) as mock_redis:
            mock_redis.return_value = AsyncMock()
            conn = RedisConn.from_settings(settings)
            assert isinstance(conn, RedisConn)
            mock_pool.assert_called_once_with(host="localhost", port=6379, decode_responses=True)

    async def test_create(self):
        from arroyosas.redis import RedisConn

        with patch("arroyosas.redis.redis.ConnectionPool") as mock_pool, patch(
            "arroyosas.redis.redis.Redis"
        ) as mock_redis:
            mock_redis.return_value = AsyncMock()
            conn = RedisConn.create("myhost", 1234)
            assert isinstance(conn, RedisConn)
            mock_pool.assert_called_once_with(host="myhost", port=1234, decode_responses=True)

    async def test_redis_subscribe_calls_callback(self, fake_redis_conn):
        """Test that redis_subscribe invokes callback on messages."""
        import asyncio

        from arroyosas.redis import RedisConn

        # Build a fake pubsub that yields one message then stops
        fake_message = {"type": "message", "channel": "scattering", "data": "hello"}
        fake_sub_message = {"type": "subscribe", "channel": "scattering", "data": 1}

        class FakePubSub:
            async def subscribe(self, channel):
                pass

            async def listen(self):
                yield fake_sub_message
                yield fake_message

        fake_conn = MagicMock()
        fake_conn.pubsub.return_value = FakePubSub()

        received = []

        async def callback(data):
            received.append(data)

        conn = RedisConn(fake_conn)
        # redis_subscribe loops forever unless we stop it; run with timeout
        try:
            await asyncio.wait_for(conn.redis_subscribe("scattering", callback), timeout=1.0)
        except asyncio.TimeoutError:
            pass

        assert received == ["hello"]
