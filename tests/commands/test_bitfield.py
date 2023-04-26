from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_bitfield_get() -> None:
    async with redis:
        # With integer offset.
        assert await redis.bitfield("string").get(encoding="u8", offset=0).execute() == [116]

        # With string offset.
        assert await redis.bitfield("string").get(encoding="u8", offset="#1").execute() == [101]


@mark.asyncio
async def test_bitfield_set() -> None:
    async with redis:
        # With integer offset.
        assert await redis.bitfield("string_for_bitfield_set").set(encoding="u8", offset=0, value=97).execute() == [116]

        # With string offset.
        assert await (
            redis.bitfield("string_for_bitfield_set")
            .set(encoding="u8", offset="#1", value=115)
            .execute()
        ) == [101]


@mark.asyncio
async def test_bitfield_incrby() -> None:
    async with redis:
        # With integer offset.
        assert await (
            redis.bitfield("string_for_bitfield_incrby")
            .incrby(encoding="u8", offset=0, increment=1)
            .execute()
        ) == [117]

        # With string offset.
        assert await (
            redis.bitfield("string_for_bitfield_incrby")
            .incrby(encoding="u8", offset="#1", increment=2)
            .execute()
        ) == [103]


@mark.asyncio
async def test_bitfield_chained_commands() -> None:
    async with redis:
        assert await (
            redis.bitfield("string_for_bitfield_chained_commands")
            .set(encoding="u8", offset=0, value=97)
            .incrby(encoding="u8", offset=0, increment=1)
            .execute()
        ) == [116, 98]


@mark.asyncio
async def test_bitfield_overflow() -> None:
    async with redis:
        assert await (
            redis.bitfield("string_for_bitfield_sat_overflow")
            .incrby(encoding="u8", offset=100, increment=1)
            .overflow("SAT")
            .incrby(encoding="u8", offset=102, increment=1)
            .execute()
        ) == [1, 5]
