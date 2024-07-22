import pytest

from upstash_redis import AsyncRedis, Redis


@pytest.mark.parametrize("redis", [{"read_your_writes": True}], indirect=True)
def test_should_update_sync_token_on_basic_request(redis: Redis):
    initial_token = redis._sync_token
    redis.set("key", "value")
    updated_token = redis._sync_token
    assert initial_token != updated_token


@pytest.mark.parametrize("async_redis", [{"read_your_writes": True}], indirect=True)
@pytest.mark.asyncio
async def test_should_update_sync_token_on_basic_request_async(async_redis: AsyncRedis):
    initial_token = async_redis._sync_token
    await async_redis.set("key", "value")
    updated_token = async_redis._sync_token
    assert initial_token != updated_token


@pytest.mark.parametrize("redis", [{"read_your_writes": True}], indirect=True)
def test_should_update_sync_token_on_pipeline(redis: Redis):
    initial_token = redis._sync_token

    pipeline = redis.pipeline()
    pipeline.set("key", "value")
    pipeline.set("key2", "value2")
    pipeline.exec()

    updated_token = redis._sync_token
    assert initial_token != updated_token


@pytest.mark.parametrize("async_redis", [{"read_your_writes": True}], indirect=True)
@pytest.mark.asyncio()
async def test_should_update_sync_token_on_pipeline_async(async_redis: AsyncRedis):
    initial_token = async_redis._sync_token

    pipeline = async_redis.pipeline()
    pipeline.set("key", "value")
    pipeline.set("key2", "value2")
    await pipeline.exec()

    updated_token = async_redis._sync_token
    assert initial_token != updated_token


@pytest.mark.parametrize("redis", [{"read_your_writes": True}], indirect=True)
def test_updates_after_successful_lua_script_call(redis: Redis):
    initial_token = redis._sync_token

    redis.eval(
        """
    redis.call('SET', 'mykey', 'myvalue')
    return 1
    """
    )

    updated_token = redis._sync_token
    assert updated_token != initial_token


@pytest.mark.parametrize("async_redis", [{"read_your_writes": True}], indirect=True)
@pytest.mark.asyncio
async def test_updates_after_successful_lua_script_call_async(async_redis: AsyncRedis):
    initial_token = async_redis._sync_token

    await async_redis.eval(
        """
    redis.call('SET', 'mykey', 'myvalue')
    return 1
    """
    )

    updated_token = async_redis._sync_token
    assert updated_token != initial_token


@pytest.mark.parametrize("redis", [{"read_your_writes": False}], indirect=True)
def test_should_not_update_sync_state_with_opt_out_ryw(redis: Redis):
    initial_token = redis._sync_token
    redis.set("key", "value")
    updated_token = redis._sync_token
    assert updated_token == initial_token


@pytest.mark.parametrize("async_redis", [{"read_your_writes": False}], indirect=True)
@pytest.mark.asyncio
async def test_should_not_update_sync_state_with_opt_out_ryw_async(
    async_redis: AsyncRedis
):
    initial_token = async_redis._sync_token
    await async_redis.set("key", "value")
    updated_token = async_redis._sync_token
    assert updated_token == initial_token


def test_should_update_sync_state_with_default_behavior(redis: Redis):
    initial_token = redis._sync_token
    redis.set("key", "value")
    updated_token = redis._sync_token
    assert updated_token != initial_token


@pytest.mark.asyncio
async def test_should_update_sync_state_with_default_behavior_async(
    async_redis: AsyncRedis
):
    initial_token = async_redis._sync_token
    await async_redis.set("key", "value")
    updated_token = async_redis._sync_token
    assert updated_token != initial_token
