from pytest import mark

from tests.async_client import redis


@mark.asyncio
async def test():
    async with redis:
        assert await redis.run(["PING"]) == "PONG"
