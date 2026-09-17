"""Tests for array commands (AR*), async client."""

import random
import string

from pytest import mark

from upstash_redis.asyncio import Redis


def random_key() -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"test-array-{suffix}"


@mark.asyncio
async def test_array_commands_async(async_redis: Redis) -> None:
    key = random_key()
    try:
        assert await async_redis.arinsert(key, "a", "b", "c") == 2
        assert await async_redis.arget(key, 1) == "b"
        assert await async_redis.armget(key, 0, 2) == ["a", "c"]
        assert await async_redis.arscan(key, 0, 10, limit=2) == [(0, "a"), (1, "b")]
        assert await async_redis.argrep(key, "-", "+", glob="[ab]") == [0, 1]
        assert await async_redis.arop(key, 0, 10, "MATCH", "c") == 1
        assert (await async_redis.arinfo(key))["count"] == 3
        assert await async_redis.arseek(key, 0) is True
        assert await async_redis.ardelrange(key, (0, 1)) == 2
        assert await async_redis.arcount(key) == 1
    finally:
        await async_redis.delete(key)


@mark.asyncio
async def test_array_pipeline_async(async_redis: Redis) -> None:
    key = random_key()
    try:
        pipeline = async_redis.pipeline()
        pipeline.arring(key, 2, "a", "b", "c")
        pipeline.arlastitems(key, 2)
        pipeline.arnext(key)
        assert await pipeline.exec() == [0, ["b", "c"], 1]
    finally:
        await async_redis.delete(key)
