from upstash_redis.asyncio import Redis

redis = Redis.from_env(allow_telemetry=False)
