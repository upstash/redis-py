from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_get() -> None:
    async with redis:
        # With integer offset.
        assert await redis.bitfield_ro("string").get(encoding="u8", offset=0).execute() == [116]

        # With string offset.
        assert await redis.bitfield_ro("string").get(encoding="u8", offset="#1").execute() == [101]


@mark.asyncio
async def test_chained_commands() -> None:
    async with redis:
        assert (
            await redis.bitfield_ro("string")
            .get(encoding="u8", offset=0)
            .get(encoding="u8", offset="#1")
            .execute()
        ) == [116, 101]
