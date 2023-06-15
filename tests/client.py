from upstash_redis.aio import Redis

redis = Redis.from_env(allow_telemetry=False)
