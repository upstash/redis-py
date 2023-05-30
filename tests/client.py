from upstash_redis.client import Redis

redis = Redis.from_env(allow_telemetry=False)
