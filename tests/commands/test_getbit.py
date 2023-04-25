from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_getbit() -> None:
    async with redis:
        assert await redis.getbit(key="string", offset=1) == 1
