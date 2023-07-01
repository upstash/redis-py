from pytest import mark
from tests.async_client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.rename("string_for_rename", newkey="rename") == "OK"

        assert await execute_on_http("GET", "rename") == "test"
