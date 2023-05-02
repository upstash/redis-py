from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_ttl() -> None:
    async with redis:
        # "string" exists but doesn't have an expiry time set.
        assert await redis.ttl("string") == -1
