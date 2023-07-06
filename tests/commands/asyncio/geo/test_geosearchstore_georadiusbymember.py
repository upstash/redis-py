from pytest import mark, raises

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadiusbymember_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadiusbymember_dist",
            "test_geo_index",
            member="Catania",
            unit="KM",
            radius=200,
        )
        == 2
    )
    assert await async_redis.zcard("geosearchstore_georadiusbymember_dist") == 2


@mark.asyncio
async def test_with_box(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadiusbymember_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadiusbymember_dist",
            "test_geo_index",
            member="Catania",
            height=20000000,
            width=40000000,
            unit="FT",
        )
        == 2
    )
    assert await async_redis.zcard("geosearchstore_georadiusbymember_dist") == 2


@mark.asyncio
async def test_with_distance(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadiusbymember_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadiusbymember_dist",
            "test_geo_index",
            member="Catania",
            unit="KM",
            radius=200,
            storedist=True,
        )
        == 2
    )
    assert await async_redis.zcard("geosearchstore_georadiusbymember_dist") == 2


@mark.asyncio
async def test_with_count(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadiusbymember_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadiusbymember_dist",
            "test_geo_index",
            member="Catania",
            unit="KM",
            radius=200,
            count=1,
        )
        == 1
    )
    assert await async_redis.zcard("geosearchstore_georadiusbymember_dist") == 1


@mark.asyncio
async def test_with_any(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadiusbymember_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadiusbymember_dist",
            "test_geo_index",
            member="Catania",
            radius=200,
            unit="KM",
            count=1,
            any=True,
        )
        == 1
    )
    assert await async_redis.zcard("geosearchstore_georadiusbymember_dist") == 1


@mark.asyncio
async def test_with_sort(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadiusbymember_dist")
    assert (
        await async_redis.geosearchstore(
            "geosearchstore_georadiusbymember_dist",
            "test_geo_index",
            member="Catania",
            radius=200,
            unit="KM",
            order="ASC",
        )
        == 2
    )
    assert await async_redis.zcard("geosearchstore_georadiusbymember_dist") == 2


@mark.asyncio
async def test_with_invalid_parameters(async_redis: AsyncRedis) -> None:
    await async_redis.delete("geosearchstore_georadiusbymember_dist")
    with raises(Exception) as exception:
        await async_redis.geosearchstore(
            "geosearchstore_georadiusbymember_dist",
            "test_geo_index",
            member="Catania",
            radius=200,
            unit="KM",
            count=None,
            any=True,
        )

    assert str(exception.value) == '"any" can only be used together with "count".'
