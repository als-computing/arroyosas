import json
import logging

import redis.asyncio as redis
from arroyosas.config import settings
from arroyosas.log_utils import setup_logger

logger = logging.getLogger(__name__)
setup_logger(logger, log_level=settings.logging_level)


class RedisConn:
    def __init__(self, redis_conn: redis.Redis):
        self.redis_conn = redis_conn

    async def get(self, key: str):
        return await self.redis_conn.get(key)

    async def set(self, key: str, value):
        await self.redis_conn.set(key, value)

    async def get_json(self, key: str):
        value_s = await self.redis_conn.get(key)
        if value_s:
            values_js = json.loads(value_s)
        else:
            values_js = {}
        return values_js

    async def set_json(self, key: str, value: dict):
        value_s = json.dumps(value)
        await self.reddis_conn.set(key, value_s)

    async def redis_subscribe(self, channel_name: str, callback: callable) -> None:
        """Listens for messages on a Redis Pub/Sub channel asynchronously."""
        pubsub = self.redis_conn.pubsub()
        channel_name = "scattering"
        await pubsub.subscribe(channel_name)  # Subscribe to the channel

        logger.info(f"Listening for messages on '{channel_name}'...")

        async for message in pubsub.listen():
            if (
                message["type"] == "message"
            ):  # Ignore subscription confirmation messages
                if message["channel"] == channel_name:
                    await callback(message["data"])

    @classmethod
    def from_settings(cls, settings: dict) -> "RedisConn":
        pool = redis.ConnectionPool(
            host=settings.host, port=settings.port, decode_responses=True
        )
        redis_conn = redis.Redis(connection_pool=pool)
        return cls(redis_conn)

    @classmethod
    def create(cls, host: str, port: int) -> "RedisConn":
        pool = redis.ConnectionPool(
            host=host, port=port, decode_responses=True
        )
        redis_conn = redis.Redis(connection_pool=pool)
        return cls(redis_conn)