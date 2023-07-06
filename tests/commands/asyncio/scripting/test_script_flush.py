import pytest

from upstash_redis import AsyncRedis


@pytest.fixture(autouse=True)
async def load_scripts(async_redis: AsyncRedis):
    await async_redis.script_flush()
    yield
    await async_redis.script_flush()


@pytest.mark.asyncio
async def test_script_flush(async_redis: AsyncRedis):
    script1 = await async_redis.script_load("return 1")
    script2 = await async_redis.script_load("return 2")

    result = await async_redis.script_exists(script1, script2)
    expected_result = [True, 1]
    assert result == expected_result

    await async_redis.script_flush()

    result = await async_redis.script_exists(script1, script2)

    expected_result = [False, False]
    assert result == expected_result
