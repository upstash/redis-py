from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_bitfield_get():
    async with redis:
        # Test with integer offset.
        assert await redis.bitfield("string_for_bitfield_get").get("u8", 0).execute() == [116]

        # Test with string offset.
        assert await redis.bitfield("string_for_bitfield_get").get("u8", "#1").execute() == [101]


@mark.asyncio
async def test_bitfield_set():
    async with redis:
        # Test with integer offset.
        assert await redis.bitfield("string_for_bitfield_set").set("u8", 0, 97).execute() == [116]

        # Test with string offset.
        assert await redis.bitfield("string_for_bitfield_set").set("u8", "#1", 115).execute() == [101]


@mark.asyncio
async def test_bitfield_incrby():
    async with redis:
        # Test with integer offset.
        assert await redis.bitfield("string_for_bitfield_incrby").incrby("u8", 0, 1).execute() == [117]

        # Test with string offset.
        assert await redis.bitfield("string_for_bitfield_incrby").incrby("u8", "#1", 2).execute() == [103]


@mark.asyncio
async def test_bitfield_chained_commands():
    async with redis:
        assert (
            await redis.bitfield("string_for_bitfield_chained_commands")
            .set("u8", 0, 97)
            .incrby("u8", 0, 1)
            .execute()
        ) == [116, 98]


@mark.asyncio
async def test_bitfield_overflow():
    async with redis:
        assert (
            await redis.bitfield("string_for_bitfield_sat_overflow")
            .incrby("u8", 100, 1)
            .overflow("SAT")
            .incrby("u8", 102, 1)
            .execute()
        ) == [1, 5]

        assert (
            await redis.bitfield("string_for_bitfield_wrap_overflow")
            .incrby("u8", 100, 1)
            .overflow("WRAP")
            .incrby("u8", 102, 1)
            .execute()
        ) == [1, 5]

        assert (
            await redis.bitfield("string_for_bitfield_fail_overflow")
            .incrby("u8", 100, 1)
            .overflow("FAIL")
            .incrby("u8", 102, 1)
            .execute()
        ) == [1, 5]
