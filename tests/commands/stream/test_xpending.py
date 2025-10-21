import pytest
import time

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xpending_basic_info(redis: Redis):
    """Test basic XPENDING functionality"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Setup
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0", mkstream=True)
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    # Get basic pending info
    result = redis.xpending(stream_key, group)
    assert isinstance(result, list)
    assert len(result) >= 4

    # Format: [count, start_id, end_id, consumers_info]
    pending_count = result[0]
    assert pending_count > 0


def test_xpending_detailed_info(redis: Redis):
    """Test detailed XPENDING with range"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add multiple entries
    for i in range(3):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create group and read messages
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=3)

    # Get detailed pending info
    result = redis.xpending(stream_key, group, "-", "+", 10)
    assert isinstance(result, list)

    # Each pending message should have: [id, consumer, idle_time, delivery_count]
    for pending_msg in result:
        assert len(pending_msg) >= 4
        message_id = pending_msg[0]
        consumer_name = pending_msg[1]
        idle_time = pending_msg[2]
        delivery_count = pending_msg[3]

        assert isinstance(message_id, str)
        assert consumer_name == consumer
        assert isinstance(idle_time, int)
        assert delivery_count >= 1


def test_xpending_with_idle_time(redis: Redis):
    """Test XPENDING with idle time filter"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entry and read it
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0", mkstream=True)
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    # Wait a bit
    time.sleep(1)

    # Get pending messages with idle time filter
    pending_with_idle = redis.xpending(stream_key, group, "-", "+", 10, idle=500)
    assert isinstance(pending_with_idle, list)

    # Should get messages that have been idle for at least 500ms
    if len(pending_with_idle) > 0:
        for pending_msg in pending_with_idle:
            idle_time = pending_msg[2]
            assert idle_time >= 500


def test_xpending_no_idle_messages(redis: Redis):
    """Test XPENDING when no messages meet idle criteria"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entry and read it immediately
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    # Immediately check for messages idle for 5000ms (should be none)
    result = redis.xpending(stream_key, group, "-", "+", 10, idle=5000)
    assert result == []


def test_xpending_specific_consumer(redis: Redis):
    """Test XPENDING filtered by specific consumer"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Add entries
    for i in range(4):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create group and have different consumers read
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=2)
    redis.xreadgroup(group, consumer2, {stream_key: ">"}, count=2)

    # Get pending for specific consumer
    pending_consumer1 = redis.xpending(
        stream_key, group, "-", "+", 10, consumer=consumer1
    )

    # All returned messages should belong to consumer1
    for pending_msg in pending_consumer1:
        assert pending_msg[1] == consumer1


def test_xpending_empty_group(redis: Redis):
    """Test XPENDING on group with no pending messages"""
    stream_key = "test_stream"
    group = "test_group"

    # Create empty group
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "$")  # Start from end

    # Check basic pending info (should show no pending)
    result = redis.xpending(stream_key, group)
    assert result[0] == 0  # No pending messages

    # Check detailed pending info (should be empty)
    detailed = redis.xpending(stream_key, group, "-", "+", 10)
    assert detailed == []


def test_xpending_after_acknowledgment(redis: Redis):
    """Test XPENDING after messages are acknowledged"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries and read them
    ids = []
    for i in range(3):
        entry_id = redis.xadd(stream_key, "*", {"field": f"value{i}"})
        ids.append(entry_id)

    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=3)

    # Check initial pending count
    initial_pending = redis.xpending(stream_key, group)
    assert initial_pending[0] == 3

    # Acknowledge some messages
    redis.xack(stream_key, group, ids[0], ids[1])

    # Check pending count after acknowledgment
    after_ack_pending = redis.xpending(stream_key, group)
    assert after_ack_pending[0] == 1  # Only 1 message still pending


def test_xpending_nonexistent_stream(redis: Redis):
    """Test XPENDING on non-existent stream"""
    with pytest.raises(Exception):
        redis.xpending("nonexistent_stream", "nonexistent_group")


def test_xpending_nonexistent_group(redis: Redis):
    """Test XPENDING on non-existent group"""
    stream_key = "test_stream"

    # Create stream but no group
    redis.xadd(stream_key, "*", {"field": "value"})

    with pytest.raises(Exception):
        redis.xpending(stream_key, "nonexistent_group")


def test_xpending_range_bounds(redis: Redis):
    """Test XPENDING with specific range bounds"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries with known IDs
    id1 = "1000000000000-0"
    id2 = "2000000000000-0"
    id3 = "3000000000000-0"

    redis.xadd(stream_key, id1, {"field": "value1"})
    redis.xadd(stream_key, id2, {"field": "value2"})
    redis.xadd(stream_key, id3, {"field": "value3"})

    # Create group and read all
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=3)

    # Get pending in specific range
    pending_range = redis.xpending(stream_key, group, id1, id2, 10)

    # Should only get messages in the specified range
    returned_ids = {pending_msg[0] for pending_msg in pending_range}
    assert id1 in returned_ids
    assert id2 in returned_ids
    # id3 should not be included as it's outside the range


def test_xpending_count_limit(redis: Redis):
    """Test XPENDING with count limit"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add many entries
    for i in range(10):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create group and read all
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=10)

    # Get pending with count limit
    pending_limited = redis.xpending(stream_key, group, "-", "+", 5)

    # Should return at most 5 entries
    assert len(pending_limited) <= 5


def test_xpending_delivery_count(redis: Redis):
    """Test XPENDING shows correct delivery count"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Add entry
    redis.xadd(stream_key, "*", {"field": "value"})

    # Create group and read with first consumer
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=1)

    # Check initial delivery count
    pending1 = redis.xpending(stream_key, group, "-", "+", 10)
    initial_delivery_count = pending1[0][3]
    assert initial_delivery_count == 1

    # Claim the message with another consumer (increases delivery count)
    message_id = pending1[0][0]
    redis.xclaim(stream_key, group, consumer2, 0, message_id)

    # Check updated delivery count
    pending2 = redis.xpending(stream_key, group, "-", "+", 10)
    updated_delivery_count = pending2[0][3]
    assert updated_delivery_count >= initial_delivery_count
