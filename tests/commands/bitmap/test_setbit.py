from pytest import mark
from tests.client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.setbit("setbit", offset=4, value=1) == 0

        assert await execute_on_http("GETBIT", "setbit", "4") == 1
