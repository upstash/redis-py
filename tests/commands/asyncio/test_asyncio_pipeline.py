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
        result = await pipeline.exec()

        # add a command to the pipeline which will be
        # removed from the pipeline when we exit the context
        pipeline.set("foo", "bar")

    assert result == [1, 2, 1, 3, 2, 4]
    assert len(pipeline._command_stack) == 0 # pipeline is empty

    # redis still works after pipeline is done
    get_result = await async_redis.get("rocket")
    assert get_result == "4"

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

@pytest.mark.asyncio
async def test_run_pipeline_twice(async_redis: Redis):
    """
    Runs a pipeline twice
    """
    pipeline = async_redis.pipeline()
    pipeline.incr("albatros")
    result = await pipeline.exec()
    assert result == [1]

    pipeline.incrby("albatros", 2)
    result = await pipeline.exec()
    assert result == [3]
