from pytest import mark
from tests.async_client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        await execute_on_http("EXPIRE", "string_for_ttl", "500")

        # < 1000 would rather indicate seconds.
        assert await redis.pttl("string_for_ttl") > 1000
