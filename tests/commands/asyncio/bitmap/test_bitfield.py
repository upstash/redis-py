from pytest import mark
from tests.async_client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test_get() -> None:
    async with redis:
        # With integer offset.
        assert await redis.bitfield("string").get(
            encoding="u8", offset=0
        ).execute() == [116]

        # With string offset.
        assert await redis.bitfield("string").get(
            encoding="u8", offset="#1"
        ).execute() == [101]


@mark.asyncio
async def test_set() -> None:
    async with redis:
        # With integer offset.
        assert await redis.bitfield("string_for_bitfield_set").set(
            encoding="u8", offset=0, value=97
        ).execute() == [116]

        assert await execute_on_http(
            "BITFIELD", "string_for_bitfield_set", "GET", "u8", "0"
        ) == [97]

        # With string offset.
        assert await (
            redis.bitfield("string_for_bitfield_set")
            .set(encoding="u8", offset="#1", value=115)
            .execute()
        ) == [101]

        assert await execute_on_http(
            "BITFIELD", "string_for_bitfield_set", "GET", "u8", "#1"
        ) == [115]


@mark.asyncio
async def test_incrby() -> None:
    async with redis:
        # With integer offset.
        assert await (
            redis.bitfield("string_for_bitfield_incrby")
            .incrby(encoding="u8", offset=0, increment=1)
            .execute()
        ) == [117]

        assert await execute_on_http(
            "BITFIELD", "string_for_bitfield_incrby", "GET", "u8", "0"
        ) == [117]

        # With string offset.
        assert await (
            redis.bitfield("string_for_bitfield_incrby")
            .incrby(encoding="u8", offset="#1", increment=2)
            .execute()
        ) == [103]

        assert await execute_on_http(
            "BITFIELD", "string_for_bitfield_incrby", "GET", "u8", "#1"
        ) == [103]


@mark.asyncio
async def test_chained_commands() -> None:
    async with redis:
        assert await (
            redis.bitfield("string_for_bitfield_chained_commands")
            .set(encoding="u8", offset=0, value=97)
            .incrby(encoding="u8", offset=0, increment=1)
            .execute()
        ) == [116, 98]

        assert await execute_on_http(
            "BITFIELD", "string_for_bitfield_chained_commands", "GET", "u8", "0"
        ) == [98]


@mark.asyncio
async def test_overflow() -> None:
    async with redis:
        assert await (
            redis.bitfield("string_for_bitfield_overflow")
            .incrby(encoding="i8", offset=100, increment=100)
            .overflow("SAT")
            .incrby(encoding="i8", offset=100, increment=100)
            .execute()
        ) == [100, 127]

        assert await execute_on_http(
            "BITFIELD", "string_for_bitfield_overflow", "GET", "i8", "100"
        ) == [127]
