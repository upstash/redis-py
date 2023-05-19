from pytest import mark, raises
from tests.client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.georadius("test_geo_index", longitude=15, latitude=37, radius=200, unit="KM") == [
            "Palermo",
            "Catania"
        ]


@mark.asyncio
async def test_with_distance() -> None:
    async with redis:
        assert await redis.georadius(
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            with_distance=True
        ) == [
            {"member": "Palermo", "distance": 190.4424},
            {"member": "Catania", "distance": 56.4413}
        ]


@mark.asyncio
async def test_with_hash() -> None:
    async with redis:
        assert await redis.georadius(
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            with_hash=True
        ) == [
            {"member": "Palermo", "hash": 3479099956230698},
            {"member": "Catania", "hash": 3479447370796909}
        ]


@mark.asyncio
async def test_with_coordinates() -> None:
    async with redis:
        assert await redis.georadius(
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            with_coordinates=True
        ) == [
            {"member": "Palermo", "longitude": 13.361389338970184, "latitude": 38.115556395496299},
            {"member": "Catania", "longitude": 15.087267458438873, "latitude": 37.50266842333162}
        ]


@mark.asyncio
async def test_with_count() -> None:
    async with redis:
        assert await redis.georadius(
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            count=1
        ) == [
            "Catania"
        ]


@mark.asyncio
async def test_with_any() -> None:
    async with redis:
        assert await redis.georadius(
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            count=1,
            count_any=True
        ) == [
            "Palermo"
        ]


@mark.asyncio
async def test_with_sort() -> None:
    async with redis:
        assert await redis.georadius(
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            sort="ASC"
        ) == [
            "Catania",
            "Palermo"
        ]


@mark.asyncio
async def test_with_store() -> None:
    async with redis:
        assert await redis.georadius(
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            store_as="test_geo_store"
        ) == 2


@mark.asyncio
async def test_with_store_dist() -> None:
    async with redis:
        assert await redis.georadius(
            "test_geo_index",
            longitude=15,
            latitude=37,
            radius=200,
            unit="KM",
            store_distance_as="test_geo_store_dist"
        ) == 2


@mark.asyncio
async def test_with_invalid_parameters() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.georadius(
                "test_geo_index",
                longitude=15,
                latitude=37,
                radius=200,
                unit="KM",
                count=None,
                count_any=True
            )

        assert str(exception.value) == "\"count_any\" can only be used together with \"count\"."


@mark.asyncio
async def test_without_allowing_deprecated_commands() -> None:
    redis.allow_deprecated = False

    async with redis:
        with raises(Exception) as exception:
            await redis.georadius(
                "test_geo_index",
                longitude=15,
                latitude=37,
                radius=200,
                unit="KM",
                count=None,
                count_any=False
            )

    assert str(exception.value) == """From version 6.2.0, this command is deprecated.
It can be replaced by "geosearch" and "geosearchstore" with the "radius" argument.
Source: https://redis.io/commands/georadius"""