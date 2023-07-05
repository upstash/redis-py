from pytest import mark

from tests.async_client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", 'return "hello world"')

    async with redis:
        assert await redis.evalsha(sha1_digest) == "hello world"


@mark.asyncio
async def test_with_keys() -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", "return {KEYS[1], KEYS[2]}")

    async with redis:
        assert await redis.evalsha(sha1_digest, keys=["a", "b"]) == ["a", "b"]


@mark.asyncio
async def test_with_arguments() -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", "return {ARGV[1], ARGV[2]}")

    async with redis:
        assert await redis.evalsha(sha1_digest, args=["c", "d"]) == ["c", "d"]


@mark.asyncio
async def test_with_keys_and_arguments() -> None:
    sha1_digest = await execute_on_http("SCRIPT", "LOAD", "return {ARGV[1], KEYS[1]}")

    async with redis:
        assert await redis.evalsha(sha1_digest, keys=["a"], args=["b"]) == ["b", "a"]
