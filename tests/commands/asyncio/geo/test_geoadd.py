from pytest import mark, raises

from tests.execute_on_http import execute_on_http
from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert (
        await async_redis.geoadd(
            "Geo",
            (13.361389, 38.115556, "Palermo"),
        )
        == 1
    )

    # Test if the key was set, and it's a Geospatial index.
    assert (
        await execute_on_http("GEODIST", "test_geo_index", "Palermo", "Catania")
        == "166274.1516"
    )


@mark.asyncio
async def test_with_nx(async_redis: AsyncRedis) -> None:
    assert (
        await async_redis.geoadd(
            "test_geo_index",
            (15.087268, 37.502669, "Catania"),
            nx=True,
        )
        == 0
    )


@mark.asyncio
async def test_with_xx(async_redis: AsyncRedis) -> None:
    assert (
        await async_redis.geoadd(
            "test_geo_index",
            (15.087268, 37.502669, "new_member"),
            xx=True,
        )
        == 0
    )


@mark.asyncio
async def test_with_ch(async_redis: AsyncRedis) -> None:
    assert (
        await async_redis.geoadd(
            "test_geo_index",
            (43.486392, -35.283347, "random"),
            ch=True,
        )
        == 1
    )


@mark.asyncio
async def test_without_members(async_redis: AsyncRedis) -> None:
    with raises(Exception) as exception:
        await async_redis.geoadd("test_geo_index")

    assert str(exception.value) == "At least one member must be added."


@mark.asyncio
async def test_with_nx_and_xx(async_redis: AsyncRedis) -> None:
    with raises(Exception) as exception:
        await async_redis.geoadd(
            "test_geo_index",
            (15.087268, 37.502669, "new_member"),
            nx=True,
            xx=True,
        )

    assert str(exception.value) == '"nx" and "xx" are mutually exclusive.'
