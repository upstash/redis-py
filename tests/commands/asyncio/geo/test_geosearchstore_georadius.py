from pytest import mark, raises

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadius_dist",
            "test_geo_index",
            fromlonlat_longitude=15,
            fromlonlat_latitude=37,
            byradius=200,
            unit="KM",
        )
        == 2
    )
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 2


@mark.asyncio
async def test_with_box(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
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
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 1


@mark.asyncio
async def test_with_distance(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
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
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 2


@mark.asyncio
async def test_with_count(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
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
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 1


@mark.asyncio
async def test_with_any(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
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
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 1


@mark.asyncio
async def test_with_sort(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
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
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 2


@mark.asyncio
async def test_with_invalid_parameters(async_redis: AsyncRedis) -> None:
    with raises(Exception) as exception:
        await async_redis.geosearchstore(
            "geosearchstore_georadius_dist",
            "test_geo_index",
            fromlonlat_longitude=15,
            fromlonlat_latitude=37,
            byradius=200,
            unit="KM",
            count=None,
            count_any=True,
        )

    assert str(exception.value) == '"count_any" can only be used together with "count".'
