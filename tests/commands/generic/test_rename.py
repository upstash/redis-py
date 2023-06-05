from pytest import mark
from tests.client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.rename("string_for_rename", new_name="rename") == "OK"

        assert await execute_on_http("GET", "rename") == "test"
