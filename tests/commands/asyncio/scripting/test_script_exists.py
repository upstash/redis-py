import pytest

from upstash_redis.asyncio import Redis


@pytest.fixture(autouse=True)
async def flush_scripts(async_redis: Redis):
    await async_redis.script_flush()
    yield
    await async_redis.script_flush()


@pytest.mark.asyncio
async def test_script_exists(async_redis: Redis):
    sha1 = await async_redis.script_load("return 1")
    sha2 = await async_redis.script_load("return 2")

    result = await async_redis.script_exists(sha1, sha2)

    expected_result = [True, 1]
    assert result == expected_result

    result = await async_redis.script_exists("non_existing_sha")

    expected_result = [False]
    assert result == expected_result
