from typing import List

from pytest import mark

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    result = await async_redis.scan(cursor=0)
    assert isinstance(result[0], int) and isinstance(result[1], List)


@mark.asyncio
async def test_with_match(async_redis: AsyncRedis) -> None:
    assert await async_redis.scan(cursor=0, match_pattern="hash") == [0, ["hash"]]


@mark.asyncio
async def test_with_count(async_redis: AsyncRedis) -> None:
    assert len(await async_redis.scan(cursor=0, count=1)) == 2


@mark.asyncio
async def test_with_scan_type(async_redis: AsyncRedis) -> None:
    assert (await async_redis.scan(cursor=0, scan_type="hash"))[1] == ["hash"]


@mark.asyncio
async def test_without_formatting(async_redis: AsyncRedis) -> None:
    async_redis._format_return = False

    result = await async_redis.scan(cursor=0)
    assert isinstance(result[0], str) and isinstance(result[1], List)
