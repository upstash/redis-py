from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert await async_redis.pfadd("pfadd", 1, "a") is True

    assert await execute_on_http("PFCOUNT", "pfadd") == 2


@mark.asyncio
async def test_without_formatting(async_redis: AsyncRedis) -> None:
    async_redis._format_return = False

    assert await async_redis.pfadd("hyperloglog", "element_1") == 0
