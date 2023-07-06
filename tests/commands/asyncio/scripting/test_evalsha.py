from typing import cast

from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", 'return "hello world"')

    assert isinstance(sha1_digest, str)
    assert await async_redis.evalsha(sha1_digest) == "hello world"


@mark.asyncio
async def test_with_keys(async_redis: AsyncRedis) -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", "return {KEYS[1], KEYS[2]}")

    assert isinstance(sha1_digest, str)
    assert await async_redis.evalsha(sha1_digest, keys=["a", "b"]) == ["a", "b"]


@mark.asyncio
async def test_with_arguments(async_redis: AsyncRedis) -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", "return {ARGV[1], ARGV[2]}")

    assert isinstance(sha1_digest, str)
    assert await async_redis.evalsha(sha1_digest, args=["c", "d"]) == ["c", "d"]


@mark.asyncio
async def test_with_keys_and_arguments(async_redis: AsyncRedis) -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", "return {ARGV[1], KEYS[1]}")

    assert isinstance(sha1_digest, str)
    assert await async_redis.evalsha(sha1_digest, keys=["a"], args=["b"]) == ["b", "a"]
