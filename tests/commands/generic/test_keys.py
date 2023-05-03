from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_keys() -> None:
    async with redis:
        assert await redis.keys(pattern="hash") == ["hash"]
