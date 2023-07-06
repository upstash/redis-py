from pytest import mark, raises

from upstash_redis import AsyncRedis


# GEORADIUSBYMEMBER tests in GEOSEARCH
@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert await async_redis.geosearch(
        "test_geo_index", member="Catania", unit="KM", radius=200
    ) == ["Palermo", "Catania"]


@mark.asyncio
async def test_with_box(async_redis: AsyncRedis) -> None:
    assert await async_redis.geosearch(
        "test_geo_index",
        member="Catania",
        height=20000000,
        width=40000000,
        unit="FT",
    ) == ["Palermo", "Catania"]


@mark.asyncio
async def test_with_distance(async_redis: AsyncRedis) -> None:
    assert await async_redis.geosearch(
        "test_geo_index",
        member="Catania",
        unit="KM",
        radius=200,
        withdist=True,
    ) == [
        {"member": "Palermo", "distance": 166.2742},
        {"member": "Catania", "distance": 0.0},
    ]


@mark.asyncio
async def test_with_hash(async_redis: AsyncRedis) -> None:
    assert await async_redis.geosearch(
        "test_geo_index",
        member="Catania",
        radius=200,
        unit="KM",
        withhash=True,
    ) == [
        {"member": "Palermo", "hash": 3479099956230698},
        {"member": "Catania", "hash": 3479447370796909},
    ]


@mark.asyncio
async def test_with_coordinates(async_redis: AsyncRedis) -> None:
    assert await async_redis.geosearch(
        "test_geo_index",
        member="Catania",
        radius=200,
        unit="KM",
        withcoord=True,
    ) == [
        {
            "member": "Palermo",
            "longitude": 13.3613893389701841,
            "latitude": 38.115556395496299,
        },
        {
            "member": "Catania",
            "longitude": 15.087267458438873,
            "latitude": 37.50266842333162,
        },
    ]


@mark.asyncio
async def test_with_count(async_redis: AsyncRedis) -> None:
    assert await async_redis.geosearch(
        "test_geo_index", member="Catania", unit="KM", radius=200, count=1
    ) == ["Catania"]


@mark.asyncio
async def test_with_any(async_redis: AsyncRedis) -> None:
    assert await async_redis.geosearch(
        "test_geo_index",
        member="Catania",
        radius=200,
        unit="KM",
        count=1,
        any=True,
    ) == ["Palermo"]


@mark.asyncio
async def test_with_sort(async_redis: AsyncRedis) -> None:
    assert await async_redis.geosearch(
        "test_geo_index",
        member="Catania",
        radius=200,
        unit="KM",
        order="ASC",
    ) == ["Catania", "Palermo"]


@mark.asyncio
async def test_with_invalid_parameters(async_redis: AsyncRedis) -> None:
    with raises(Exception) as exception:
        await async_redis.geosearch(
            "test_geo_index",
            member="Catania",
            radius=200,
            unit="KM",
            count=None,
            any=True,
        )

    assert str(exception.value) == '"any" can only be used together with "count".'
