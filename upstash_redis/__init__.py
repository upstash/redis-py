__version__ = "1.6.0"

from upstash_redis.asyncio.client import Redis as AsyncRedis
from upstash_redis.client import Redis

__all__ = ["AsyncRedis", "Redis"]
