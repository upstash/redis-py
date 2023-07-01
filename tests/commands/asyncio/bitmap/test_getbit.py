from pytest import mark
from tests.async_client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.getbit(key="string", offset=1) == 1
