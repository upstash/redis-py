from pytest import mark, raises

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.unlink("string_for_unlink_1", "string_for_unlink_2") == 2


@mark.asyncio
async def test_without_keys(async_redis: Redis) -> None:
    with raises(Exception) as exception:
        await async_redis.unlink()

    assert str(exception.value) == "At least one key must be specified."
