from pytest import mark
from tests.client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.geopos("test_geo_index", "Palermo") == [
            {"longitude": 13.361389338970184, "latitude": 38.115556395496299}
        ]


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.geopos("test_geo_index", "Palermo") == [
            ["13.361389338970184", "38.115556395496299"]
        ]

    redis.format_return = True
