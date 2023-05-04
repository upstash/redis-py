from upstash_py.client import Redis
from os import environ

redis = Redis(
    url=environ["UPSTASH_REDIS_REST_URL"],
    token=environ["UPSTASH_REDIS_REST_TOKEN"],
    allow_deprecated=True
)
