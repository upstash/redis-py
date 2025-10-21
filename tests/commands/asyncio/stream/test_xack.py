import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()


@pytest.mark.asyncio
async def test_xack_basic(async_redis: Redis):
    """Test basic XACK functionality"""
    # Add entries
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xadd("mystream", "*", {"field": "value2"})

    # Create consumer group
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read messages as consumer
    result = await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})
    entries = result[0][1]

    # Get message IDs
    id1 = entries[0][0]

    # Acknowledge one message
    acked = await async_redis.xack("mystream", "mygroup", id1)
    assert acked == 1

    # Check pending messages (should be 1 left)
    pending = await async_redis.xpending("mystream", "mygroup")
    assert pending[0] == 1


@pytest.mark.asyncio
async def test_xack_multiple_ids(async_redis: Redis):
    """Test XACK with multiple message IDs"""
    # Add entries
    for i in range(5):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Create consumer group
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read messages as consumer
    result = await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})
    entries = result[0][1]

    # Get first 3 message IDs
    ids = [entry[0] for entry in entries[:3]]

    # Acknowledge multiple messages at once
    acked = await async_redis.xack("mystream", "mygroup", *ids)
    assert acked == 3

    # Check pending messages (should be 2 left)
    pending = await async_redis.xpending("mystream", "mygroup")
    assert pending[0] == 2


@pytest.mark.asyncio
async def test_xack_nonexistent_message(async_redis: Redis):
    """Test XACK with non-existent message ID"""
    # Add entry and create group
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Try to acknowledge non-existent message
    acked = await async_redis.xack("mystream", "mygroup", "9999999999999-0")
    assert acked == 0


@pytest.mark.asyncio
async def test_xack_already_acknowledged(async_redis: Redis):
    """Test XACK on already acknowledged message"""
    # Add entry and create group
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read and acknowledge message
    result = await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})
    message_id = result[0][1][0][0]

    # First acknowledgment
    acked = await async_redis.xack("mystream", "mygroup", message_id)
    assert acked == 1

    # Second acknowledgment (already acked)
    acked = await async_redis.xack("mystream", "mygroup", message_id)
    assert acked == 0


@pytest.mark.asyncio
async def test_xack_different_consumers(async_redis: Redis):
    """Test XACK with messages from different consumers"""
    # Add entries
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xadd("mystream", "*", {"field": "value2"})

    # Create consumer group
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read with consumer1
    result1 = await async_redis.xreadgroup(
        "mygroup", "consumer1", {"mystream": ">"}, count=1
    )
    id1 = result1[0][1][0][0]

    # Read with consumer2
    result2 = await async_redis.xreadgroup(
        "mygroup", "consumer2", {"mystream": ">"}, count=1
    )
    id2 = result2[0][1][0][0]

    # Both consumers can acknowledge their respective messages
    acked1 = await async_redis.xack("mystream", "mygroup", id1)
    acked2 = await async_redis.xack("mystream", "mygroup", id2)

    assert acked1 == 1
    assert acked2 == 1

    # No pending messages left
    pending = await async_redis.xpending("mystream", "mygroup")
    assert pending[0] == 0
