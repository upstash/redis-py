from pytest import mark, raises
from tests.client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert (
            await redis.geoadd(
                "Geo",
                {"longitude": 13.361389, "latitude": 38.115556, "member": "Palermo"},
            )
            == 1
        )

        # Test if the key was set, and it's a Geospatial index.
        assert (
            await execute_on_http("GEODIST", "test_geo_index", "Palermo", "Catania")
            == "166274.1516"
        )


@mark.asyncio
async def test_with_nx() -> None:
    async with redis:
        assert (
            await redis.geoadd(
                "test_geo_index",
                {"longitude": 15.087268, "latitude": 37.502669, "member": "Catania"},
                nx=True,
            )
            == 0
        )


@mark.asyncio
async def test_with_xx() -> None:
    async with redis:
        assert (
            await redis.geoadd(
                "test_geo_index",
                {"longitude": 15.087268, "latitude": 37.502669, "member": "new_member"},
                xx=True,
            )
            == 0
        )


@mark.asyncio
async def test_with_ch() -> None:
    async with redis:
        assert (
            await redis.geoadd(
                "test_geo_index",
                {"longitude": 43.486392, "latitude": -35.283347, "member": "random"},
                ch=True,
            )
            == 1
        )


@mark.asyncio
async def test_without_members() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.geoadd("test_geo_index")

        assert str(exception.value) == "At least one member must be added."


@mark.asyncio
async def test_with_nx_and_xx() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.geoadd(
                "test_geo_index",
                {"longitude": 15.087268, "latitude": 37.502669, "member": "new_member"},
                nx=True,
                xx=True,
            )

        assert str(exception.value) == '"nx" and "xx" are mutually exclusive.'
