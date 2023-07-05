from pytest import mark

from tests.async_client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.pfadd("pfadd", 1, "a") is True

        assert await execute_on_http("PFCOUNT", "pfadd") == 2


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.pfadd("hyperloglog", "element_1") == 0

    redis.format_return = True
