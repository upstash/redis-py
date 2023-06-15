from pytest import mark
from tests.client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        result: list[int | list[str]] = await redis.scan(cursor=0)
        assert isinstance(result[0], int) and isinstance(result[1], list)


@mark.asyncio
async def test_with_match() -> None:
    async with redis:
        assert await redis.scan(cursor=0, match="hash") == [0, ["hash"]]


@mark.asyncio
async def test_with_count() -> None:
    async with redis:
        assert len(await redis.scan(cursor=0, count=1)) == 2


@mark.asyncio
async def test_with_scan_type() -> None:
    async with redis:
        assert (await redis.scan(cursor=0, scan_type="hash"))[1] == ["hash"]


@mark.asyncio
async def test_without_returning_cursor() -> None:
    async with redis:
        assert isinstance(await redis.scan(cursor=0, return_cursor=False), list)


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        result: list[int | list[str]] = await redis.scan(cursor=0)
        assert isinstance(result[0], str) and isinstance(result[1], list)

    redis.format_return = True
