import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()


@pytest.mark.asyncio
async def test_xpending_summary(async_redis: Redis):
    """Test XPENDING summary form"""
    # Add entries and create group
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xadd("mystream", "*", {"field": "value2"})
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read messages to create pending entries
    await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})

    # Get summary pending info
    pending = await async_redis.xpending("mystream", "mygroup")

    assert isinstance(pending, list)
    assert pending[0] == 2  # Should have 2 pending messages


@pytest.mark.asyncio
async def test_xpending_detailed(async_redis: Redis):
    """Test XPENDING detailed form"""
    # Add entries and create group
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xadd("mystream", "*", {"field": "value2"})
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read messages to create pending entries
    await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})

    # Get detailed pending info - all three parameters are required
    detailed = await async_redis.xpending("mystream", "mygroup", "-", "+", 10)

    assert isinstance(detailed, list)
    assert len(detailed) == 2  # Should have 2 pending messages


@pytest.mark.asyncio
async def test_xpending_detailed_with_consumer(async_redis: Redis):
    """Test XPENDING detailed form with specific consumer"""
    # Add entries and create group
    for i in range(3):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read messages with different consumers
    await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"}, count=2)
    await async_redis.xreadgroup("mygroup", "consumer2", {"mystream": ">"}, count=1)

    # Get detailed pending info for specific consumer
    detailed = await async_redis.xpending(
        "mystream", "mygroup", "-", "+", 10, "consumer1"
    )

    assert isinstance(detailed, list)
    assert len(detailed) == 2  # consumer1 should have 2 pending messages


@pytest.mark.asyncio
async def test_xpending_detailed_with_idle(async_redis: Redis):
    """Test XPENDING detailed form with IDLE parameter"""
    # Add entries and create group
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read message to create pending entry
    await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})

    # Get detailed pending info with IDLE filter (0ms minimum idle time)
    detailed = await async_redis.xpending("mystream", "mygroup", "-", "+", 10, idle=0)

    assert isinstance(detailed, list)
    assert len(detailed) >= 1


@pytest.mark.asyncio
async def test_xpending_partial_args_error(async_redis: Redis):
    """Test that providing partial detailed args raises an error"""
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Providing only start should raise an error
    with pytest.raises(
        ValueError, match="start, end, and count must all be provided together"
    ):
        await async_redis.xpending("mystream", "mygroup", start="-")

    # Providing only start and end should raise an error
    with pytest.raises(
        ValueError, match="start, end, and count must all be provided together"
    ):
        await async_redis.xpending("mystream", "mygroup", start="-", end="+")

    # Providing only count should raise an error
    with pytest.raises(
        ValueError, match="start, end, and count must all be provided together"
    ):
        await async_redis.xpending("mystream", "mygroup", count=10)


@pytest.mark.asyncio
async def test_xpending_empty_pending_list(async_redis: Redis):
    """Test XPENDING when no messages are pending"""
    # Add entry and create group but don't read any messages
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xgroup_create("mystream", "mygroup", "$")  # Start from end

    # Summary should show no pending messages
    pending = await async_redis.xpending("mystream", "mygroup")
    assert pending[0] == 0

    # Detailed should return empty list
    detailed = await async_redis.xpending("mystream", "mygroup", "-", "+", 10)
    assert detailed == []
