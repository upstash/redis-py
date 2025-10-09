import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()


@pytest.mark.asyncio
async def test_xlen_basic(async_redis: Redis):
    """Test basic XLEN functionality"""
    # Initially, stream doesn't exist
    length = await async_redis.xlen("mystream")
    assert length == 0

    # Add entries and check length
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    length = await async_redis.xlen("mystream")
    assert length == 1

    # Add more entries
    await async_redis.xadd("mystream", "*", {"field": "value2"})
    await async_redis.xadd("mystream", "*", {"field": "value3"})
    length = await async_redis.xlen("mystream")
    assert length == 3


@pytest.mark.asyncio
async def test_xlen_after_deletion(async_redis: Redis):
    """Test XLEN after deleting entries"""
    # Add entries
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    id2 = await async_redis.xadd("mystream", "*", {"field": "value2"})
    await async_redis.xadd("mystream", "*", {"field": "value3"})

    # Initial length
    length = await async_redis.xlen("mystream")
    assert length == 3

    # Delete one entry
    deleted = await async_redis.xdel("mystream", id2)
    assert deleted == 1

    # Length should decrease
    length = await async_redis.xlen("mystream")
    assert length == 2


@pytest.mark.asyncio
async def test_xlen_after_trimming(async_redis: Redis):
    """Test XLEN after trimming stream"""
    # Add multiple entries
    for i in range(130):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Initial length
    length = await async_redis.xlen("mystream")
    assert length == 130

    # Trim to 5 entries
    trimmed = await async_redis.xtrim("mystream", maxlen=5)
    assert trimmed >= 0

    # Length should be 5
    length = await async_redis.xlen("mystream")
    assert length == 5
