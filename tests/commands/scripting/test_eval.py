from pytest import mark
from tests.client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.eval('return "hello world"') == "hello world"


@mark.asyncio
async def test_with_keys() -> None:
    async with redis:
        assert await redis.eval("return {KEYS[1], KEYS[2]}", keys=["a", "b"]) == [
            "a",
            "b",
        ]


@mark.asyncio
async def test_with_arguments() -> None:
    async with redis:
        assert await redis.eval("return {ARGV[1], ARGV[2]}", args=["c", "d"]) == [
            "c",
            "d",
        ]


@mark.asyncio
async def test_with_keys_and_arguments() -> None:
    async with redis:
        assert await redis.eval(
            "return {ARGV[1], KEYS[1]}", keys=["a"], args=["b"]
        ) == ["b", "a"]
