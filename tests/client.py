from upstash_redis.client import Redis

redis = Redis.from_env(allow_deprecated=True, allow_telemetry=False)
