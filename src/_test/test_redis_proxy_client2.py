"""Additional tests for arroyosas.lse_reduction.redis_proxy_client covering subscribe."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestRedisHTTPClientSubscribe:
    """Tests for the subscribe async generator (lines 69-105)."""

    async def test_subscribe_yields_parsed_json(self):
        """Test subscribe yields parsed JSON objects."""
        from arroyosas.lse_reduction.redis_proxy_client import RedisHTTPClient

        # Build SSE lines that form a valid event
        sse_lines = [
            'data: {"key": "value"}',
            "",  # blank line signals end of event
        ]

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        async def mock_aiter_lines():
            for line in sse_lines:
                yield line

        mock_resp.aiter_lines = mock_aiter_lines

        # Create a context manager for stream
        class MockStreamContext:
            async def __aenter__(self):
                return mock_resp

            async def __aexit__(self, *args):
                pass

        mock_http_client = MagicMock()
        mock_http_client.stream = MagicMock(return_value=MockStreamContext())

        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_http_client
            c = RedisHTTPClient(base_url="http://localhost:8000")
            c.client = mock_http_client

            results = []
            async for item in c.subscribe("my_channel"):
                results.append(item)

        assert len(results) == 1
        assert results[0] == {"key": "value"}

    async def test_subscribe_yields_raw_text_on_json_error(self):
        """Test subscribe yields raw text when JSON parse fails."""
        from arroyosas.lse_reduction.redis_proxy_client import RedisHTTPClient

        sse_lines = [
            "data: not-valid-json",
            "",
        ]

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        async def mock_aiter_lines():
            for line in sse_lines:
                yield line

        mock_resp.aiter_lines = mock_aiter_lines

        class MockStreamContext:
            async def __aenter__(self):
                return mock_resp

            async def __aexit__(self, *args):
                pass

        mock_http_client = MagicMock()
        mock_http_client.stream = MagicMock(return_value=MockStreamContext())

        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_http_client
            c = RedisHTTPClient(base_url="http://localhost:8000")
            c.client = mock_http_client

            results = []
            async for item in c.subscribe("my_channel"):
                results.append(item)

        assert len(results) == 1
        assert results[0] == "not-valid-json"

    async def test_subscribe_skips_heartbeat_events(self):
        """Test that blank-line-only events (heartbeats) are skipped."""
        from arroyosas.lse_reduction.redis_proxy_client import RedisHTTPClient

        # heartbeat: blank line with no data lines before it
        sse_lines = [
            "",  # heartbeat - no data lines before it
            'data: {"real": "data"}',
            "",
        ]

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        async def mock_aiter_lines():
            for line in sse_lines:
                yield line

        mock_resp.aiter_lines = mock_aiter_lines

        class MockStreamContext:
            async def __aenter__(self):
                return mock_resp

            async def __aexit__(self, *args):
                pass

        mock_http_client = MagicMock()
        mock_http_client.stream = MagicMock(return_value=MockStreamContext())

        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_http_client
            c = RedisHTTPClient(base_url="http://localhost:8000")
            c.client = mock_http_client

            results = []
            async for item in c.subscribe("my_channel"):
                results.append(item)

        # Only the real data event, not the heartbeat
        assert len(results) == 1
        assert results[0] == {"real": "data"}

    async def test_subscribe_skips_non_data_lines(self):
        """Test that lines without 'data:' prefix are ignored in the payload."""
        from arroyosas.lse_reduction.redis_proxy_client import RedisHTTPClient

        # event: has id and comment but no data -> empty data string -> skipped
        sse_lines = [
            ": this is a comment",
            "id: 12345",
            "",
        ]

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        async def mock_aiter_lines():
            for line in sse_lines:
                yield line

        mock_resp.aiter_lines = mock_aiter_lines

        class MockStreamContext:
            async def __aenter__(self):
                return mock_resp

            async def __aexit__(self, *args):
                pass

        mock_http_client = MagicMock()
        mock_http_client.stream = MagicMock(return_value=MockStreamContext())

        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_http_client
            c = RedisHTTPClient(base_url="http://localhost:8000")
            c.client = mock_http_client

            results = []
            async for item in c.subscribe("my_channel"):
                results.append(item)

        # Should be skipped (no data: lines)
        assert len(results) == 0

    async def test_subscribe_uses_token_in_header(self):
        """Test that token is included in subscribe request headers."""
        from arroyosas.lse_reduction.redis_proxy_client import RedisHTTPClient

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        async def mock_aiter_lines():
            return
            yield  # make it an async generator

        mock_resp.aiter_lines = mock_aiter_lines

        captured_headers = {}

        class MockStreamContext:
            async def __aenter__(self):
                return mock_resp

            async def __aexit__(self, *args):
                pass

        mock_http_client = MagicMock()

        def mock_stream(method, url, params=None, headers=None):
            captured_headers.update(headers or {})
            return MockStreamContext()

        mock_http_client.stream = mock_stream
        mock_http_client.headers = {}

        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_http_client
            c = RedisHTTPClient(base_url="http://localhost:8000", token="mytoken")
            c.client = mock_http_client

            async for _ in c.subscribe("my_channel"):
                pass

        assert captured_headers.get("Authorization") == "Bearer mytoken"

    async def test_get_raises_for_non_404_error(self):
        """Test that get() calls raise_for_status on non-404 errors."""
        from arroyosas.lse_reduction.redis_proxy_client import RedisHTTPClient

        mock_http = AsyncMock()
        mock_http.headers = {}

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status = MagicMock(side_effect=Exception("server error"))
        mock_http.get = AsyncMock(return_value=mock_response)

        with patch("arroyosas.lse_reduction.redis_proxy_client.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_http
            c = RedisHTTPClient(base_url="http://localhost:8000")
            c.client = mock_http

            with pytest.raises(Exception, match="server error"):
                await c.get("some_key")
