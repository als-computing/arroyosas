import json

import redis

KEY_CURRENT_REDUCTION_PARAMS = "current_reduction_params"


class KVStore:
    def __init__(self, redis_server: redis.Redis):
        self.redis_server = redis.Redis(host="localhost", port=6379, db=0)

    def get(self, key: str):
        return self.redis_server.get(key)

    def set(self, key: str, value):
        self.redis_server.set(key, value)

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
        redis_server = redis.Redis(host=settings.host, port=settings.port)
        return cls(redis_server)
