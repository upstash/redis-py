from pytest import mark
from tests.client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.pfmerge("pfmerge", "hyperloglog_for_pfmerge_1", "hyperloglog_for_pfmerge_2") == "OK"

        assert await execute_on_http("PFCOUNT", "pfmerge") == 4
