from pytest import mark

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert (
        await async_redis.geodist("test_geo_index", "Palermo", "Catania") == 166274.1516
    )


@mark.asyncio
async def test_with_unit(async_redis: AsyncRedis) -> None:
    assert (
        await async_redis.geodist("test_geo_index", "Palermo", "Catania", unit="km")
        == 166.2742
    )


@mark.asyncio
async def test_without_formatting(async_redis: AsyncRedis) -> None:
    async_redis._format_return = False

    assert (
        await async_redis.geodist("test_geo_index", "Palermo", "Catania")
        == "166274.1516"
    )
