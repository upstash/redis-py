from pytest import mark, raises

from upstash_redis.asyncio import Redis
from upstash_redis.utils import GeoSearchResult


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.georadius_ro(
        "test_geo_index", longitude=15, latitude=37, radius=200, unit="KM"
    ) == ["Palermo", "Catania"]


@mark.asyncio
async def test_with_distance(async_redis: Redis) -> None:
    assert await async_redis.georadius_ro(
        "test_geo_index",
        longitude=15,
        latitude=37,
        radius=200,
        unit="KM",
        withdist=True,
    ) == [
        GeoSearchResult(member="Palermo", distance=190.4424),
        GeoSearchResult(member="Catania", distance=56.4413),
    ]


@mark.asyncio
async def test_with_hash(async_redis: Redis) -> None:
    assert await async_redis.georadius_ro(
        "test_geo_index",
        longitude=15,
        latitude=37,
        radius=200,
        unit="KM",
        withhash=True,
    ) == [
        GeoSearchResult(member="Palermo", hash=3479099956230698),
        GeoSearchResult(member="Catania", hash=3479447370796909),
    ]


@mark.asyncio
async def test_with_coordinates(async_redis: Redis) -> None:
    assert await async_redis.georadius_ro(
        "test_geo_index",
        longitude=15,
        latitude=37,
        radius=200,
        unit="KM",
        withcoord=True,
    ) == [
        GeoSearchResult(
            member="Palermo",
            longitude=13.361389338970184,
            latitude=38.115556395496299,
        ),
        GeoSearchResult(
            member="Catania",
            longitude=15.087267458438873,
            latitude=37.50266842333162,
        ),
    ]


@mark.asyncio
async def test_with_count(async_redis: Redis) -> None:
    assert await async_redis.georadius_ro(
        "test_geo_index", longitude=15, latitude=37, radius=200, unit="KM", count=1
    ) == ["Catania"]


@mark.asyncio
async def test_with_any(async_redis: Redis) -> None:
    assert await async_redis.georadius_ro(
        "test_geo_index",
        longitude=15,
        latitude=37,
        radius=200,
        unit="KM",
        count=1,
        any=True,
    ) == ["Palermo"]


@mark.asyncio
async def test_with_sort(async_redis: Redis) -> None:
    assert await async_redis.georadius_ro(
        "test_geo_index",
        longitude=15,
        latitude=37,
        radius=200,
        unit="KM",
        order="ASC",
    ) == ["Catania", "Palermo"]


@mark.asyncio
async def test_with_invalid_parameters(async_redis: Redis) -> None:
    with raises(Exception) as exception:
        await async_redis.georadius_ro(
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            count=None,
            any=True,
        )

    assert str(exception.value) == '"any" can only be used together with "count".'
