from typing import List

from pytest import mark

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    result = await async_redis.scan(cursor=0)
    assert isinstance(result[0], int) and isinstance(result[1], List)


@mark.asyncio
async def test_with_match(async_redis: Redis) -> None:
    assert await async_redis.scan(cursor=0, match="hash") == (0, ["hash"])


@mark.asyncio
async def test_with_count(async_redis: Redis) -> None:
    assert len(await async_redis.scan(cursor=0, count=1)) == 2


@mark.asyncio
async def test_with_scan_type(async_redis: Redis) -> None:
    assert (await async_redis.scan(cursor=0, type="hash"))[1] == ["hash"]


@mark.asyncio
async def test_scan_multiple_times(async_redis: Redis) -> None:
    cursor, keys = await async_redis.scan(cursor=0, count=1)
    assert cursor != 0
    assert len(keys) == 1
    cursor, keys = await async_redis.scan(cursor=cursor, count=1)
    assert cursor != 0
    assert len(keys) == 1
