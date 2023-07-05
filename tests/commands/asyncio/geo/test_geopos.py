from pytest import mark

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert await async_redis.geopos("test_geo_index", "Palermo") == [
        {"longitude": 13.361389338970184, "latitude": 38.115556395496299}
    ]


@mark.asyncio
async def test_without_formatting(async_redis: AsyncRedis) -> None:
    async_redis._format_return = False

    assert await async_redis.geopos("test_geo_index", "Palermo") == [
        ["13.361389338970184", "38.115556395496299"]
    ]
