from pytest import mark

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test_get(async_redis: Redis) -> None:
    # With integer offset.
    assert await async_redis.bitfield_ro("string").get(
        encoding="u8", offset=0
    ).execute() == [116]

    # With string offset.
    assert await async_redis.bitfield_ro("string").get(
        encoding="u8", offset="#1"
    ).execute() == [101]


@mark.asyncio
async def test_chained_commands(async_redis: Redis) -> None:
    assert await async_redis.bitfield_ro("string").get(encoding="u8", offset=0).get(
        encoding="u8", offset="#1"
    ).execute() == [116, 101]
