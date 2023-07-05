from pytest import mark

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert await async_redis.eval('return "hello world"') == "hello world"


@mark.asyncio
async def test_with_keys(async_redis: AsyncRedis) -> None:
    assert await async_redis.eval("return {KEYS[1], KEYS[2]}", keys=["a", "b"]) == [
        "a",
        "b",
    ]


@mark.asyncio
async def test_with_arguments(async_redis: AsyncRedis) -> None:
    assert await async_redis.eval("return {ARGV[1], ARGV[2]}", args=["c", "d"]) == [
        "c",
        "d",
    ]


@mark.asyncio
async def test_with_keys_and_arguments(async_redis: AsyncRedis) -> None:
    assert await async_redis.eval(
        "return {ARGV[1], KEYS[1]}", keys=["a"], args=["b"]
    ) == [
        "b",
        "a",
    ]
