from pytest import mark, raises
from tests.client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.pfcount("hyperloglog") == 2


@mark.asyncio
async def test_without_keys() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.pfcount()

        assert str(exception.value) == "At least one key must be specified."
