from pytest import mark, raises
from tests.client import redis


@mark.asyncio
async def test_not_none_operation() -> None:
    async with redis:
        assert await (
            redis.bitop("AND", "bitop_destination_1", "string_as_bitop_source_1", "string_as_bitop_source_2")
        ) == 4


@mark.asyncio
async def test_without_source_keys() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.bitop("AND", "bitop_destination_1")

        assert str(exception.value) == "At least one source key must be specified."


@mark.asyncio
async def test_not() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.bitop("NOT", "bitop_destination_4", "string_as_bitop_source_1", "string_as_bitop_source_2")

        assert str(exception.value) == "The \"NOT \" operation takes only one source key as argument."

        assert await redis.bitop("NOT", "bitop_destination_4", "string_as_bitop_source_1") == 4
