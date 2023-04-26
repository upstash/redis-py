from pytest import mark, raises
from tests.client import redis


@mark.asyncio
async def test_exists_without_keys() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.exists()

        assert str(exception.value) == "At least one key must be checked."


@mark.asyncio
async def test_exists() -> None:
    async with redis:
        assert await redis.exists("string", "hash") == 2
