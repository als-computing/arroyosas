import logging
import os

from arroyosas.lse_reduction.redis_model_store import RedisModelStore
from .lse_reduction.operator import LatentSpaceOperator
from .lse_reduction.reducer import LatentSpaceReducer

logger = logging.getLogger(__name__)

def build_lse_operator(redis_host: str = None, redis_port: int = None) -> LatentSpaceOperator:
            
    # Initialize RedisModelStore instead of direct Redis client
    try:
        redis_host = redis_host or os.getenv("REDIS_HOST", "kvrocks")
        redis_port = redis_port or int(os.getenv("REDIS_PORT", 6666))
        print(redis_host)
        redis_model_store = RedisModelStore(host=redis_host, port=redis_port)
        logger.info(f"Connected to Redis Model Store at {redis_host}:{redis_port}")
    except Exception as e:
        logger.warning(f"Could not connect to Redis Model Store: {e}")
        redis_model_store = None
    reducer = LatentSpaceReducer(redis_model_store)
    return LatentSpaceOperator(reducer, redis_model_store)
