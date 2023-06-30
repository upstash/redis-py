from upstash_redis.asyncio import Redis
from upstash_redis import Redis as SyncRedis

redis = Redis.from_env(allow_telemetry=False)
syncRedis = SyncRedis.from_env(allow_telemetry=False)
