import pytest
from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis.errors import UpstashError
from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", 'return "hello world"')

    assert isinstance(sha1_digest, str)
    assert await async_redis.evalsha_ro(sha1_digest) == "hello world"


@mark.asyncio
async def test_with_keys(async_redis: Redis) -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", "return {KEYS[1], KEYS[2]}")

    assert isinstance(sha1_digest, str)
    assert await async_redis.evalsha_ro(sha1_digest, keys=["a", "b"]) == ["a", "b"]


@mark.asyncio
async def test_with_arguments(async_redis: Redis) -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", "return {ARGV[1], ARGV[2]}")

    assert isinstance(sha1_digest, str)
    assert await async_redis.evalsha_ro(sha1_digest, args=["c", "d"]) == ["c", "d"]


@mark.asyncio
async def test_with_keys_and_arguments(async_redis: Redis) -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", "return {ARGV[1], KEYS[1]}")

    assert isinstance(sha1_digest, str)
    assert await async_redis.evalsha_ro(sha1_digest, keys=["a"], args=["b"]) == [
        "b",
        "a",
    ]


@mark.asyncio
async def test_with_readonly_evalsha(async_redis: Redis) -> None:
    key = "mykey"
    value = "Hello, Redis!"

    await async_redis.set(key, value)
    sha1_digest = await execute_on_http(
        "SCRIPT", "LOAD", "return redis.call('GET', KEYS[1])"
    )

    assert isinstance(sha1_digest, str)
    assert await async_redis.evalsha_ro(sha1_digest, keys=[key]) == value


@mark.asyncio
async def test_with_write_evalsha(async_redis: Redis) -> None:
    key = "mykey"
    value = "Hello, Redis!"

    await async_redis.set(key, value)
    sha1_digest = await execute_on_http(
        "SCRIPT", "LOAD", "return redis.call('DEL', KEYS[1])"
    )

    assert isinstance(sha1_digest, str)
    with pytest.raises(UpstashError):
        await async_redis.evalsha_ro(sha1_digest, keys=[key])
