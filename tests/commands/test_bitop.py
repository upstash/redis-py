from pytest import mark, raises
from tests.client import redis


@mark.asyncio
async def test_bitop_and() -> None:
    async with redis:
        assert await redis.bitop("AND", "destination_1", "string_for_bitop_source_1", "string_for_bitop_source_2") == 4


@mark.asyncio
async def test_bitop_or() -> None:
    async with redis:
        assert await redis.bitop("OR", "destination_2", "string_for_bitop_source_1", "string_for_bitop_source_2") == 4


@mark.asyncio
async def test_bitop_xor() -> None:
    async with redis:
        assert await redis.bitop("XOR", "destination_3", "string_for_bitop_source_1", "string_for_bitop_source_2") == 4


@mark.asyncio
async def test_bitop_not() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.bitop("NOT", "destination_2", "string_for_bitop_source_1", "string_for_bitop_source_2")

        assert str(exception.value) == "The \"NOT \" operation takes only one source key as argument."

        assert await redis.bitop("NOT", "destination_4", "string_for_bitop_source_1") == 4
