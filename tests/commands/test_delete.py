from pytest import mark, raises
from tests.client import redis


@mark.asyncio
async def test_delete_without_keys() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.delete()

        assert str(exception.value) == "At least one key must be deleted."


@mark.asyncio
async def test_delete() -> None:
    async with redis:
        assert await redis.delete("string_for_delete") == 1

        # This key does not exist.
        assert await redis.delete("delete") == 0