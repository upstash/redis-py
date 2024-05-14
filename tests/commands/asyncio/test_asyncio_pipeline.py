import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.delete("rocket", "space", "marine")

@pytest.mark.asyncio
async def test_pipeline(async_redis: Redis):

    pipeline = async_redis.pipeline()

    pipeline.incr("rocket")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")

    # can chain commands
    pipeline.get("rocket").get("space").get("marine")

    res = await pipeline.exec()
    assert res == [1, 2, 1, 3, 2, 4, "4", "2", None]

@pytest.mark.asyncio
async def test_multi(async_redis: Redis):

    pipeline = async_redis.multi()

    pipeline.incr("rocket")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")
    pipeline.incr("space")
    pipeline.incr("rocket")

    pipeline.get("rocket")
    pipeline.get("space")
    pipeline.get("marine")

    res = await pipeline.exec()
    assert res == [1, 2, 1, 3, 2, 4, "4", "2", None]

@pytest.mark.asyncio
async def test_context_manager_usage(async_redis: Redis):

    async with async_redis.pipeline() as pipeline:
        pipeline.incr("rocket")
        pipeline.incr("rocket")
        pipeline.incr("space")
        pipeline.incr("rocket")
        pipeline.incr("space")
        pipeline.incr("rocket")

    # redis still works after pipeline is done
    result = await async_redis.get("rocket")
    assert result == "4"

    get_pipeline = async_redis.pipeline()
    get_pipeline.get("rocket")
    get_pipeline.get("space")
    get_pipeline.get("marine")

    res = await get_pipeline.exec()
    assert res == ["4", "2", None]

@pytest.mark.asyncio
async def test_context_manager_raise(async_redis: Redis):
    """
    Check that exceptions in context aren't silently ignored

    This can happen if we return something in __exit__ method
    """
    with pytest.raises(Exception):
        async with async_redis.pipeline() as pipeline:
            pipeline.incr("rocket")
            raise Exception("test")
