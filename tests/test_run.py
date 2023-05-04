from tests.client import redis
from pytest import mark


@mark.asyncio
async def test():
    async with redis:
        assert await redis.run(["PING"]) == "PONG"
