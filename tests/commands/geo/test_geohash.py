from pytest import mark
from tests.client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.geohash("test_geo_index", "Palermo") == ["sqc8b49rny0"]
