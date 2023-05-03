from pytest import mark, raises
from tests.client import redis


@mark.asyncio
async def test_unlink() -> None:
    async with redis:
        assert await redis.unlink("string_for_unlink_1", "string_for_unlink_2") == 2


@mark.asyncio
async def test_unlink_without_keys() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.unlink()

        assert str(exception.value) == "At least one key must be specified."
