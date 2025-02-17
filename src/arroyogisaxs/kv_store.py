import json

import redis


class KVStore:
    def __init__(self, redis_conn: redis.Redis):
        self.redis_conn = redis_conn

    def get(self, key: str):
        return self.redis_conn.get(key)

    def set(self, key: str, value):
        self.redis_conn.set(key, value)

    def get_json(self, key: str):
        value_s = self.get(key)
        if value_s:
            values_js = json.loads(value_s)
        else:
            values_js = {}
        return values_js

    def set_json(self, key: str, value: dict):
        value_s = json.dumps(value)
        self.set(key, value_s)

    @classmethod
    def from_settings(cls, settings: dict) -> "KVStore":
        pool = redis.ConnectionPool(
            host=settings.host, port=settings.port, decode_responses=True
        )
        redis_conn = redis.Redis(connection_pool=pool)
        return cls(redis_conn)
