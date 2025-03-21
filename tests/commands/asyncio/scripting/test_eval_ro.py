import pytest
from pytest import mark
from upstash_redis.errors import UpstashError
from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.eval_ro('return "hello world"') == "hello world"


@mark.asyncio
async def test_with_keys(async_redis: Redis) -> None:
    assert await async_redis.eval_ro("return {KEYS[1], KEYS[2]}", keys=["a", "b"]) == [
        "a",
        "b",
    ]


@mark.asyncio
async def test_with_arguments(async_redis: Redis) -> None:
    assert await async_redis.eval_ro("return {ARGV[1], ARGV[2]}", args=["c", "d"]) == [
        "c",
        "d",
    ]


@mark.asyncio
async def test_with_keys_and_arguments(async_redis: Redis) -> None:
    assert await async_redis.eval_ro(
        "return {ARGV[1], KEYS[1]}", keys=["a"], args=["b"]
    ) == [
        "b",
        "a",
    ]


@mark.asyncio
async def test_with_readonly_eval(async_redis: Redis) -> None:
    key = "mykey"
    value = "Hello, Redis!"

    await async_redis.set(key, value)

    assert (
        await async_redis.eval_ro("return redis.call('GET', KEYS[1])", keys=[key])
        == value
    )


@mark.asyncio
async def test_with_write_eval(async_redis: Redis) -> None:
    key = "mykey"
    value = "Hello, Redis!"

    await async_redis.set(key, value)

    with pytest.raises(UpstashError):
        await async_redis.eval_ro("return redis.call('DEL', KEYS[1])", keys=[key])
