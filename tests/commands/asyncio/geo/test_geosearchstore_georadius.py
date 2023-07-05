from pytest import mark, raises

from tests.async_client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        await redis.delete("geosearchstore_georadius_dist")
        assert (
            await redis.geosearchstore(
                "geosearchstore_georadius_dist",
                "test_geo_index",
                fromlonlat_longitude=15,
                fromlonlat_latitude=37,
                byradius=200,
                unit="KM",
            )
            == 2
        )
        assert await redis.zcard("geosearchstore_georadius_dist") == 2


@mark.asyncio
async def test_with_box() -> None:
    async with redis:
        await redis.delete("geosearchstore_georadius_dist")
        assert (
            await redis.geosearchstore(
                "geosearchstore_georadius_dist",
                "test_geo_index",
                fromlonlat_longitude=14,
                fromlonlat_latitude=35,
                bybox_height=600,
                bybox_width=4000,
                unit="km",
            )
            == 1
        )
        assert await redis.zcard("geosearchstore_georadius_dist") == 1


@mark.asyncio
async def test_with_distance() -> None:
    async with redis:
        await redis.delete("geosearchstore_georadius_dist")
        assert (
            await redis.geosearchstore(
                "geosearchstore_georadius_dist",
                "test_geo_index",
                fromlonlat_longitude=15,
                fromlonlat_latitude=37,
                byradius=200,
                unit="KM",
                storedist=True,
            )
            == 2
        )
        assert await redis.zcard("geosearchstore_georadius_dist") == 2


@mark.asyncio
async def test_with_count() -> None:
    async with redis:
        await redis.delete("geosearchstore_georadius_dist")
        assert (
            await redis.geosearchstore(
                "geosearchstore_georadius_dist",
                "test_geo_index",
                fromlonlat_longitude=15,
                fromlonlat_latitude=37,
                byradius=200,
                unit="KM",
                count=1,
            )
            == 1
        )
        assert await redis.zcard("geosearchstore_georadius_dist") == 1


@mark.asyncio
async def test_with_any() -> None:
    async with redis:
        await redis.delete("geosearchstore_georadius_dist")
        assert (
            await redis.geosearchstore(
                "geosearchstore_georadius_dist",
                "test_geo_index",
                fromlonlat_longitude=15,
                fromlonlat_latitude=37,
                byradius=200,
                unit="KM",
                count=1,
                count_any=True,
            )
            == 1
        )
        assert await redis.zcard("geosearchstore_georadius_dist") == 1


@mark.asyncio
async def test_with_sort() -> None:
    async with redis:
        await redis.delete("geosearchstore_georadius_dist")
        assert (
            await redis.geosearchstore(
                "geosearchstore_georadius_dist",
                "test_geo_index",
                fromlonlat_longitude=15,
                fromlonlat_latitude=37,
                byradius=200,
                unit="KM",
                sort="ASC",
            )
            == 2
        )
        assert await redis.zcard("geosearchstore_georadius_dist") == 2


@mark.asyncio
async def test_with_invalid_parameters() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.geosearchstore(
                "geosearchstore_georadius_dist",
                "test_geo_index",
                fromlonlat_longitude=15,
                fromlonlat_latitude=37,
                byradius=200,
                unit="KM",
                count=None,
                count_any=True,
            )

        assert (
            str(exception.value)
            == '"count_any" can only be used together with "count".'
        )
