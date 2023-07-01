from pytest import mark, raises
from tests.async_client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.delete("string_for_delete_1", "string_for_delete_2") == 2

        assert (
            await execute_on_http(
                "EXISTS", "string_for_delete_1", "string_for_delete_2"
            )
            == 0
        )


@mark.asyncio
async def test_without_keys() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.delete()

        assert str(exception.value) == "At least one key must be deleted."
