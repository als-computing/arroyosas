import json
from typing import AsyncGenerator, Optional, Union

import httpx


class RedisHTTPClient:
    """Async HTTP client for Redis HTTP Gateway (SSE subscribe).

    Methods are async: get, set, publish, subscribe (async generator), close.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        token: Optional[str] = None,
        timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        # Provide all four timeout parameters explicitly to satisfy httpx API.
        client_timeout = httpx.Timeout(connect=timeout, read=None, write=None, pool=None)
        self.client = httpx.AsyncClient(timeout=client_timeout)
        print(f"RedisHTTPClient connecting to {self.base_url}")
        if token:
            self.client.headers["Authorization"] = f"Bearer {token}"

    # --- Basic KV ops ---

    async def get(self, key: str) -> Optional[Union[dict, str]]:
        """GET /get?key=..."""
        r = await self.client.get(f"{self.base_url}/get", params={"key": key})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    async def set(
        self,
        key: str,
        value,
        ttl: Optional[int] = None,
        nx: Optional[bool] = None,
        xx: Optional[bool] = None,
    ):
        """POST /set"""
        payload = {"key": key, "value": value}
        if ttl is not None:
            payload["ttl"] = ttl
        if nx is not None:
            payload["nx"] = nx
        if xx is not None:
            payload["xx"] = xx

        r = await self.client.post(f"{self.base_url}/set", json=payload)
        r.raise_for_status()
        return r.json()

    # --- Pub/Sub ---

    async def publish(self, channel: str, message):
        """POST /publish"""
        payload = {"channel": channel, "message": message}
        r = await self.client.post(f"{self.base_url}/publish", json=payload)
        r.raise_for_status()
        return r.json()

    async def subscribe(self, channel: str, heartbeat: int = 2) -> AsyncGenerator[Union[dict, str], None]:
        """Async generator for SSE subscribe endpoint.

        Yields parsed JSON messages when possible, otherwise raw text.
        """
        url = f"{self.base_url}/subscribe"
        params = {"channel": channel, "heartbeat": heartbeat}
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        async with self.client.stream("GET", url, params=params, headers=headers) as resp:
            resp.raise_for_status()
            # Simple async SSE parser: accumulate lines until a blank line
            data_lines = []
            async for raw_line in resp.aiter_lines():
                # raw_line is a str without trailing newline
                line = raw_line.rstrip("\r\n")
                if line == "":
                    # end of event
                    if not data_lines:
                        # heartbeat or comment-only event
                        data_lines = []
                        continue
                    # concatenate data: lines starting with 'data:'
                    data = "\n".join(line_.partition(":")[2].lstrip() for line_ in data_lines if line_.startswith("data:"))
                    data_lines = []
                    if not data:
                        continue
                    try:
                        yield json.loads(data)
                    except json.JSONDecodeError:
                        yield data
                    continue

                # Collect lines (including comments). Preserve exact prefix for parsing.
                data_lines.append(line)

    async def close(self):
        await self.client.aclose()


def from_url(url: str, token: Optional[str] = None, timeout: float = 10.0) -> RedisHTTPClient:
    return RedisHTTPClient(base_url=url, token=token, timeout=timeout)
