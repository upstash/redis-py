from pytest import mark, raises

from tests.async_client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.georadiusbymember(
            "test_geo_index", "Catania", 200, "KM"
        ) == ["Palermo", "Catania"]


@mark.asyncio
async def test_with_distance() -> None:
    async with redis:
        assert await redis.georadiusbymember(
            "test_geo_index",
            "Catania",
            200,
            unit="KM",
            withdist=True,
        ) == [
            {"member": "Palermo", "distance": 166.2742},
            {"member": "Catania", "distance": 0.0},
        ]


@mark.asyncio
async def test_with_hash() -> None:
    async with redis:
        assert await redis.georadiusbymember(
            "test_geo_index",
            "Catania",
            200,
            unit="KM",
            withhash=True,
        ) == [
            {"member": "Palermo", "hash": 3479099956230698},
            {"member": "Catania", "hash": 3479447370796909},
        ]


@mark.asyncio
async def test_with_coordinates() -> None:
    async with redis:
        assert await redis.georadiusbymember(
            "test_geo_index",
            "Catania",
            200,
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
async def test_with_count() -> None:
    async with redis:
        assert await redis.georadiusbymember(
            "test_geo_index", "Catania", 200, unit="KM", count=1
        ) == ["Catania"]


@mark.asyncio
async def test_with_any() -> None:
    async with redis:
        assert await redis.georadiusbymember(
            "test_geo_index",
            "Catania",
            200,
            unit="KM",
            count=1,
            count_any=True,
        ) == ["Palermo"]


@mark.asyncio
async def test_with_sort() -> None:
    async with redis:
        assert await redis.georadiusbymember(
            "test_geo_index",
            "Catania",
            200,
            unit="KM",
            sort="ASC",
        ) == ["Catania", "Palermo"]


@mark.asyncio
async def test_with_store() -> None:
    async with redis:
        assert (
            await redis.georadiusbymember(
                "test_geo_index",
                "Catania",
                200,
                unit="KM",
                store="test_geo_store",
            )
            == 2
        )
        assert await redis.zcard("test_geo_store") == 2


@mark.asyncio
async def test_with_store_dist() -> None:
    async with redis:
        assert (
            await redis.georadiusbymember(
                "test_geo_index",
                "Catania",
                100,
                unit="KM",
                storedist="test_geo_store_dist",
            )
            == 1
        )
        assert await redis.zcard("test_geo_store_dist") == 1


@mark.asyncio
async def test_with_invalid_parameters() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.georadiusbymember(
                "test_geo_index",
                "Catania",
                200,
                unit="KM",
                count=None,
                count_any=True,
            )

        assert (
            str(exception.value)
            == '"count_any" can only be used together with "count".'
        )
