import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()


@pytest.mark.asyncio
async def test_xclaim_basic(async_redis: Redis):
    """Test basic XCLAIM functionality"""
    # Add entries and create group
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xadd("mystream", "*", {"field": "value2"})
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read messages with consumer1
    result = await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})
    message_id = result[0][1][0][0]

    # Consumer2 claims the message from consumer1
    claimed = await async_redis.xclaim(
        "mystream",
        "mygroup",
        "consumer2",
        0,  # min_idle_time = 0 (claim immediately)
        message_id,
    )

    assert isinstance(claimed, list)
    if len(claimed) > 0:
        assert claimed[0][0] == message_id


@pytest.mark.asyncio
async def test_xclaim_with_justid(async_redis: Redis):
    """Test XCLAIM with JUSTID option"""
    # Add entries and create group
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xgroup_create("mystream", "mygroup", "0")

    # Read message with consumer1
    result = await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})
    message_id = result[0][1][0][0]

    # Claim with justid option
    claimed = await async_redis.xclaim(
        "mystream",
        "mygroup",
        "consumer2",
        0,  # min_idle_time
        message_id,
        justid=True,  # return just IDs
    )

    assert isinstance(claimed, list)


@pytest.mark.asyncio
async def test_xclaim_multiple_ids(async_redis: Redis):
    """Test XCLAIM with multiple message IDs"""
    # Add multiple entries
    for i in range(3):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Create group and read messages
    await async_redis.xgroup_create("mystream", "mygroup", "0")
    result = await async_redis.xreadgroup("mygroup", "consumer1", {"mystream": ">"})

    # Get all message IDs
    message_ids = [entry[0] for entry in result[0][1]]

    # Claim all messages at once
    claimed = await async_redis.xclaim(
        "mystream",
        "mygroup",
        "consumer2",
        0,  # min_idle_time
        *message_ids,  # unpack all message IDs
    )

    assert isinstance(claimed, list)
    assert len(claimed) >= len(message_ids)
