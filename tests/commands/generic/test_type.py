from pytest import mark
from tests.client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.type("hash") == "hash"
