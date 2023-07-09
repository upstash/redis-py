import asyncio

import pytest

from upstash_redis import Redis
from upstash_redis.asyncio import Redis as AsyncRedis


def test_redis() -> None:
    redis = Redis.from_env(allow_telemetry=False)
    assert redis.ping("hey") == "hey"
    redis.close()


def test_redis_with_context_manager() -> None:
    with Redis.from_env(allow_telemetry=False) as redis:
        assert redis.ping("hey") == "hey"


@pytest.mark.asyncio
async def test_async_redis() -> None:
    redis = AsyncRedis.from_env(allow_telemetry=False)
    assert await redis.ping("hey") == "hey"
    await redis.close()


@pytest.mark.asyncio
async def test_async_redis_with_context_manager() -> None:
    async with AsyncRedis.from_env(allow_telemetry=False) as redis:
        assert await redis.ping("hey") == "hey"


def test_async_redis_init_outside_coroutine() -> None:
    redis = AsyncRedis.from_env(allow_telemetry=False)

    async def coro() -> str:
        result = await redis.ping("hey")
        await redis.close()
        return result

    assert asyncio.run(coro()) == "hey"
