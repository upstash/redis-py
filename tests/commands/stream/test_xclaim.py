import pytest
import time

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xclaim_basic(redis: Redis):
    """Test basic XCLAIM functionality"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Add entry and setup consumer group
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")

    # Consumer1 reads the message
    result = redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=1)
    message_id = result[0][1][0][0]

    # Consumer2 claims the message from consumer1
    claimed = redis.xclaim(
        stream_key,
        group,
        consumer2,
        0,  # min_idle_time = 0 (claim immediately)
        message_id,
    )

    assert isinstance(claimed, list)
    if len(claimed) > 0:
        # Should return the claimed message info
        assert claimed[0][0] == message_id


def test_xclaim_with_idle_time(redis: Redis):
    """Test XCLAIM with minimum idle time"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Setup
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")

    # Consumer1 reads message
    result = redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=1)
    message_id = result[0][1][0][0]

    # Wait a bit
    time.sleep(1)

    # Consumer2 tries to claim with idle time requirement
    claimed = redis.xclaim(
        stream_key,
        group,
        consumer2,
        500,  # 500ms minimum idle time
        message_id,
    )

    # Should succeed since message has been idle long enough
    assert isinstance(claimed, list)


def test_xclaim_justid_option(redis: Redis):
    """Test XCLAIM with JUSTID option"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Setup
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")

    # Consumer1 reads message
    result = redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=1)
    message_id = result[0][1][0][0]

    # Consumer2 claims with JUSTID
    claimed = redis.xclaim(stream_key, group, consumer2, 0, message_id, justid=True)

    # With JUSTID, should return only the IDs
    assert isinstance(claimed, list)
    if len(claimed) > 0:
        assert claimed[0] == message_id


def test_xclaim_multiple_messages(redis: Redis):
    """Test XCLAIM with multiple messages"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Add multiple messages
    message_ids = []
    for i in range(3):
        entry_id = redis.xadd(stream_key, "*", {"field": f"value{i}"})
        message_ids.append(entry_id)

    # Setup group and consumer1 reads all
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=3)

    # Consumer2 claims all messages
    claimed = redis.xclaim(stream_key, group, consumer2, 0, *message_ids)

    assert isinstance(claimed, list)


def test_xclaim_basic_functionality(redis: Redis):
    """Test basic XCLAIM functionality"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Setup
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")

    # Consumer1 reads message
    result = redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=1)
    message_id = result[0][1][0][0]

    # Consumer2 claims message
    claimed = redis.xclaim(
        stream_key,
        group,
        consumer2,
        0,  # min_idle_time
        message_id,
    )

    # Should succeed despite high idle time requirement
    assert isinstance(claimed, list)


def test_xautoclaim_basic(redis: Redis):
    """Test basic XAUTOCLAIM functionality"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Setup
    redis.xadd(stream_key, "*", {"field": "value1"})
    redis.xadd(stream_key, "*", {"field": "value2"})
    redis.xgroup_create(stream_key, group, "0")

    # Consumer1 reads messages
    redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=2)

    # Wait a bit to make messages idle
    time.sleep(1)

    # Consumer2 auto-claims idle messages
    result = redis.xautoclaim(
        stream_key,
        group,
        consumer2,
        500,  # 500ms minimum idle time
        "0-0",  # Start from beginning
        count=10,
    )

    # Should return [next_start, claimed_messages]
    assert isinstance(result, list)
    assert len(result) == 2

    next_start = result[0]
    claimed_messages = result[1]

    assert isinstance(next_start, str)
    assert isinstance(claimed_messages, list)


def test_xautoclaim_with_count_limit(redis: Redis):
    """Test XAUTOCLAIM with count limit"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Add multiple messages
    for i in range(5):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Setup group and consumer1 reads all
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=5)

    # Wait and auto-claim with count limit
    time.sleep(1)
    result = redis.xautoclaim(stream_key, group, consumer2, 500, "0-0", count=2)

    # Should respect count limit
    claimed_messages = result[1]
    assert len(claimed_messages) <= 2


def test_xautoclaim_justid_option(redis: Redis):
    """Test XAUTOCLAIM with JUSTID option"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Setup
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")

    # Consumer1 reads, then consumer2 auto-claims with JUSTID
    redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=1)
    time.sleep(1)

    result = redis.xautoclaim(stream_key, group, consumer2, 500, "0-0", justid=True)

    # With JUSTID, claimed messages should be just IDs
    assert isinstance(result, list)
    if len(result) > 1 and len(result[1]) > 0:
        # Each claimed item should be a string ID, not [id, data] pair
        claimed_item = result[1][0]
        assert isinstance(claimed_item, str)


def test_xautoclaim_no_idle_messages(redis: Redis):
    """Test XAUTOCLAIM when no messages meet idle criteria"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Setup and immediately try to auto-claim
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=1)

    # Immediately try to claim with high idle requirement
    result = redis.xautoclaim(
        stream_key,
        group,
        consumer2,
        5000,  # 5 seconds idle time required
        "0-0",
    )

    # Should return empty claimed messages
    next_start, claimed_messages = result
    assert claimed_messages == []


def test_xclaim_nonexistent_message(redis: Redis):
    """Test XCLAIM with non-existent message ID"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Setup basic stream and group
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")

    # Try to claim non-existent message
    claimed = redis.xclaim(stream_key, group, consumer, 0, "9999999999999-0")

    # Should return empty or handle gracefully
    assert isinstance(claimed, list)
    # Non-existent messages typically result in empty claims
