import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()


@pytest.mark.asyncio
async def test_xreadgroup_basic(async_redis: Redis):
    """Test basic XREADGROUP functionality"""
    # Add some entries
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xadd("mystream", "*", {"field": "value2"})

    # Create consumer group
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read as part of consumer group
    result = await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})

    assert len(result) == 1
    stream_name, entries = result[0]
    assert stream_name == "mystream"
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_xreadgroup_with_count(async_redis: Redis):
    """Test XREADGROUP with COUNT option"""
    # Add multiple entries
    for i in range(5):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Create consumer group
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read with count limit
    result = await async_redis.xreadgroup(
        "mygroup", "consumer1", {"mystream": ">"}, count=3
    )

    stream_name, entries = result[0]
    assert len(entries) == 3


@pytest.mark.asyncio
async def test_xreadgroup_with_noack(async_redis: Redis):
    """Test XREADGROUP with NOACK option"""
    # Add an entry
    await async_redis.xadd("mystream", "*", {"field": "value1"})

    # Create consumer group
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read with NOACK - message should not be added to PEL
    result = await async_redis.xreadgroup(
        "mygroup", "consumer1", {"mystream": ">"}, noack=True
    )

    assert len(result) == 1
    stream_name, entries = result[0]
    assert len(entries) == 1

    # Check pending list should be empty due to NOACK
    pending = await async_redis.xpending("mystream", "mygroup")
    assert pending[0] == 0


@pytest.mark.asyncio
async def test_xreadgroup_multiple_consumers(async_redis: Redis):
    """Test XREADGROUP with multiple consumers"""
    # Add multiple entries
    for i in range(4):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Create consumer group
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read with first consumer
    result1 = await async_redis.xreadgroup(
        "mygroup", "consumer1", {"mystream": ">"}, count=2
    )

    # Read with second consumer
    result2 = await async_redis.xreadgroup(
        "mygroup", "consumer2", {"mystream": ">"}, count=2
    )

    # Both should get different messages
    entries1 = result1[0][1]
    entries2 = result2[0][1]

    assert len(entries1) == 2
    assert len(entries2) == 2

    # Entries should be different
    entry_ids1 = {entry[0] for entry in entries1}
    entry_ids2 = {entry[0] for entry in entries2}
    assert entry_ids1.isdisjoint(entry_ids2)


@pytest.mark.asyncio
async def test_xreadgroup_from_id(async_redis: Redis):
    """Test XREADGROUP from specific ID"""
    # Add entries
    id1 = await async_redis.xadd("mystream", "*", {"field": "value1"})
    id2 = await async_redis.xadd("mystream", "*", {"field": "value2"})

    # Create consumer group starting from id1
    await async_redis.xgroup_create("mystream", "mygroup", id1)

    # Read new messages (should only get id2)
    result = await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})

    stream_name, entries = result[0]
    assert len(entries) == 1
    assert entries[0][0] == id2


@pytest.mark.asyncio
async def test_xreadgroup_no_new_messages(async_redis: Redis):
    """Test XREADGROUP when no new messages are available"""
    # Add an entry
    await async_redis.xadd("mystream", "*", {"field": "value1"})

    # Create consumer group from end
    await async_redis.xgroup_create("mystream", "mygroup", "$")

    # Try to read new messages (should be empty)
    result = await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})

    assert result == []
