from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_copy() -> None:
    async with redis:
        assert await redis.copy(source="string", destination="copy_destination") is True


@mark.asyncio
async def test_copy_with_replace() -> None:
    async with redis:
        assert await redis.copy(source="string", destination="string_as_copy_destination", replace=True) is True


@mark.asyncio
async def test_copy_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.copy(source="string", destination="copy_destination_2") == 1

    redis.format_return = True
