import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()

@pytest.mark.asyncio
async def test_pipeline(async_redis: Redis):

    pipeline = async_redis.pipeline()

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
def test_raises(async_redis: Redis):

    pipeline = async_redis.pipeline()
    with pytest.raises(NotImplementedError):
        pipeline.pipeline()
    with pytest.raises(NotImplementedError):
        pipeline.multi()

    multi = async_redis.multi()
    with pytest.raises(NotImplementedError):
        multi.pipeline()
    with pytest.raises(NotImplementedError):
        multi.multi()
