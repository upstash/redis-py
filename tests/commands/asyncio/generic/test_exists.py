from pytest import mark, raises

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.exists("string", "hash") == 2


@mark.asyncio
async def test_without_keys(async_redis: Redis) -> None:
    with raises(Exception) as exception:
        await async_redis.exists()

    assert str(exception.value) == "At least one key must be checked."
