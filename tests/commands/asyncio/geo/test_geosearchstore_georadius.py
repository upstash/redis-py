from pytest import mark, raises

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadius_dist",
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
        )
        == 2
    )
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 2


@mark.asyncio
async def test_with_box(async_redis: Redis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadius_dist",
            "test_geo_index",
            longitude=14,
            latitude=35,
            height=600,
            width=4000,
            unit="KM",
        )
        == 1
    )
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 1


@mark.asyncio
async def test_with_distance(async_redis: Redis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadius_dist",
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            storedist=True,
        )
        == 2
    )
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 2


@mark.asyncio
async def test_with_count(async_redis: Redis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadius_dist",
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            count=1,
        )
        == 1
    )
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 1


@mark.asyncio
async def test_with_any(async_redis: Redis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadius_dist",
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            count=1,
            any=True,
        )
        == 1
    )
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 1


@mark.asyncio
async def test_with_sort(async_redis: Redis) -> None:
    await async_redis.delete("geosearchstore_georadius_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadius_dist",
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            order="ASC",
        )
        == 2
    )
    assert await async_redis.zcard("geosearchstore_georadius_dist") == 2


@mark.asyncio
async def test_with_invalid_parameters(async_redis: Redis) -> None:
    with raises(Exception) as exception:
        await async_redis.geosearchstore(
            "geosearchstore_georadius_dist",
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            count=None,
            any=True,
        )

    assert str(exception.value) == '"any" can only be used together with "count".'
