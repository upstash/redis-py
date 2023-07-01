from pytest import mark, raises
from tests.async_client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.touch("string") == 1


@mark.asyncio
async def test_without_keys() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.touch()

        assert str(exception.value) == "At least one key must be specified."
