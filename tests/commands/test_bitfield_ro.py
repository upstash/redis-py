from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_bitfield_ro_get():
    async with redis:
        # Test with integer offset.
        assert await redis.bitfield_ro("string_for_bitfield_get").get("u8", 0).execute() == [116]

        # Test with string offset.
        assert await redis.bitfield_ro("string_for_bitfield_get").get("u8", "#1").execute() == [101]


@mark.asyncio
async def test_bitfield_ro_chained_commands():
    async with redis:
        assert await redis.bitfield_ro("string_for_bitfield_get").get("u8", 0).get("u8", "#1").execute() == [116, 101]
