from pytest import mark

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.geopos("test_geo_index", "Palermo") == [
        (13.361389338970184, 38.115556395496299)
    ]
