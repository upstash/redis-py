from pytest import mark
from tests.async_client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert (
            await redis.geodist("test_geo_index", "Palermo", "Catania") == 166274.1516
        )


@mark.asyncio
async def test_with_unit() -> None:
    async with redis:
        assert (
            await redis.geodist("test_geo_index", "Palermo", "Catania", unit="km")
            == 166.2742
        )


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert (
            await redis.geodist("test_geo_index", "Palermo", "Catania") == "166274.1516"
        )

    redis.format_return = True
