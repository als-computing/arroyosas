"""Tests for arroyosas.lse_reduction.redis_proxy_client (RedisHTTPClient)"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from arroyosas.lse_reduction.redis_proxy_client import RedisHTTPClient, from_url

pytestmark = pytest.mark.asyncio


@pytest.fixture
def client():
    with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
        mock_http = AsyncMock()
        mock_http.headers = {}
        mock_cls.return_value = mock_http
        c = RedisHTTPClient(base_url="http://localhost:8000", timeout=5.0)
        c._http = mock_http
        yield c


class TestRedisHTTPClientInit:
    def test_init_strips_trailing_slash(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = MagicMock(headers={})
            c = RedisHTTPClient(base_url="http://localhost:8000/")
            assert c.base_url == "http://localhost:8000"

    def test_init_with_token(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_client = MagicMock()
            mock_client.headers = {}
            mock_cls.return_value = mock_client
            RedisHTTPClient(base_url="http://localhost:8000", token="mytoken")
            assert mock_client.headers["Authorization"] == "Bearer mytoken"

    def test_init_without_token(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_client = MagicMock()
            mock_client.headers = {}
            mock_cls.return_value = mock_client
            RedisHTTPClient(base_url="http://localhost:8000")
            assert "Authorization" not in mock_client.headers


class TestRedisHTTPClientGet:
    async def test_get_returns_json(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_http = AsyncMock()
            mock_http.headers = {}
            mock_cls.return_value = mock_http

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"value": "hello"}
            mock_http.get = AsyncMock(return_value=mock_response)

            c = RedisHTTPClient(base_url="http://localhost:8000")
            result = await c.get("mykey")
            assert result == {"value": "hello"}

    async def test_get_returns_none_on_404(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_http = AsyncMock()
            mock_http.headers = {}
            mock_cls.return_value = mock_http

            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_http.get = AsyncMock(return_value=mock_response)

            c = RedisHTTPClient(base_url="http://localhost:8000")
            result = await c.get("missing_key")
            assert result is None


class TestRedisHTTPClientSet:
    async def test_set_posts_payload(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_http = AsyncMock()
            mock_http.headers = {}
            mock_cls.return_value = mock_http

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"ok": True}
            mock_response.raise_for_status = MagicMock()
            mock_http.post = AsyncMock(return_value=mock_response)

            c = RedisHTTPClient(base_url="http://localhost:8000")
            await c.set("mykey", "myvalue")
            mock_http.post.assert_called_once()
            call_kwargs = mock_http.post.call_args[1]
            assert call_kwargs["json"]["key"] == "mykey"
            assert call_kwargs["json"]["value"] == "myvalue"

    async def test_set_with_ttl(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_http = AsyncMock()
            mock_http.headers = {}
            mock_cls.return_value = mock_http

            mock_response = MagicMock()
            mock_response.json.return_value = {}
            mock_response.raise_for_status = MagicMock()
            mock_http.post = AsyncMock(return_value=mock_response)

            c = RedisHTTPClient(base_url="http://localhost:8000")
            await c.set("key", "val", ttl=60)
            payload = mock_http.post.call_args[1]["json"]
            assert payload["ttl"] == 60


class TestRedisHTTPClientPublish:
    async def test_publish(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_http = AsyncMock()
            mock_http.headers = {}
            mock_cls.return_value = mock_http

            mock_response = MagicMock()
            mock_response.json.return_value = {"receivers": 1}
            mock_response.raise_for_status = MagicMock()
            mock_http.post = AsyncMock(return_value=mock_response)

            c = RedisHTTPClient(base_url="http://localhost:8000")
            result = await c.publish("my_channel", {"data": "value"})
            assert result == {"receivers": 1}
            payload = mock_http.post.call_args[1]["json"]
            assert payload["channel"] == "my_channel"


class TestRedisHTTPClientClose:
    async def test_close(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_http = AsyncMock()
            mock_http.headers = {}
            mock_http.aclose = AsyncMock()
            mock_cls.return_value = mock_http

            c = RedisHTTPClient(base_url="http://localhost:8000")
            await c.close()
            mock_http.aclose.assert_called_once()


class TestFromUrl:
    def test_from_url(self):
        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = MagicMock(headers={})
            c = from_url("http://myserver:8080", token="tok", timeout=30.0)
            assert isinstance(c, RedisHTTPClient)
            assert c.base_url == "http://myserver:8080"
            assert c.token == "tok"
