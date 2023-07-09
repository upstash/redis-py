from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert (
        await async_redis.pfmerge(
            "pfmerge", "hyperloglog_for_pfmerge_1", "hyperloglog_for_pfmerge_2"
        )
        is True
    )

    assert await execute_on_http("PFCOUNT", "pfmerge") == 4
