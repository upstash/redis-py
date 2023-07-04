from pytest import mark

from tests.async_client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.copy(source="string", destination="copy_destination") is True

        assert await execute_on_http("GET", "copy_destination") == "test"


@mark.asyncio
async def test_with_replace() -> None:
    async with redis:
        assert (
            await redis.copy(
                source="string", destination="string_as_copy_destination", replace=True
            )
            is True
        )

        assert await execute_on_http("GET", "string_as_copy_destination") == "test"


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.copy(source="string", destination="copy_destination_2") == 1

    redis.format_return = True


@mark.asyncio
async def test_with_formatting() -> None:
    redis.format_return = True

    async with redis:
        await redis.copy(source="string", destination="copy_destination_2")
        assert (
            await redis.copy(source="string", destination="copy_destination_2") == False
        )
