import pytest

from upstash_redis.asyncio import Redis


@pytest.fixture(autouse=True)
async def flush_scripts(async_redis: Redis):
    await async_redis.script_flush()
    yield
    await async_redis.script_flush()


@pytest.mark.asyncio
async def test_script_load(async_redis: Redis):
    script1 = "return 1"
    script2 = "return 2"

    script1_sha = await async_redis.script_load(script1)
    script2_sha = await async_redis.script_load(script2)

    res = await async_redis.evalsha(script1_sha)
    assert res == 1

    result = await async_redis.script_exists(script1_sha, script2_sha)

    expected_result = [True, True]
    assert result == expected_result
