from pytest import mark

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert (
        await async_redis.geodist("test_geo_index", "Palermo", "Catania") == 166274.1516
    )


@mark.asyncio
async def test_with_unit(async_redis: Redis) -> None:
    assert (
        await async_redis.geodist("test_geo_index", "Palermo", "Catania", unit="KM")
        == 166.2742
    )
