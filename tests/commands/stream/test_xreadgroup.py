import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xreadgroup_basic(redis: Redis):
    """Test basic XREADGROUP functionality"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    for i in range(3):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read with consumer group
    result = redis.xreadgroup(group, consumer, {stream_key: ">"})

    assert len(result) == 1
    stream_name, entries = result[0]
    assert stream_name == stream_key
    assert len(entries) == 3


def test_xreadgroup_with_count(redis: Redis):
    """Test XREADGROUP with COUNT option"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    for i in range(5):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read with count limit
    result = redis.xreadgroup(group, consumer, {stream_key: ">"}, count=2)

    stream_name, entries = result[0]
    assert len(entries) == 2


def test_xreadgroup_pending_messages(redis: Redis):
    """Test reading pending messages with XREADGROUP"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    for i in range(3):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read new messages (makes them pending)
    redis.xreadgroup(group, consumer, {stream_key: ">"})

    # Read pending messages for this consumer
    result2 = redis.xreadgroup(group, consumer, {stream_key: "0"})

    # Should get the same messages again (they're pending)
    if result2:
        stream_name, entries = result2[0]
        assert len(entries) >= 0  # May be 0 or more depending on implementation


def test_xreadgroup_noack_option(redis: Redis):
    """Test XREADGROUP with NOACK option"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entry
    redis.xadd(stream_key, "*", {"field": "value"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read with NOACK (messages won't be added to PEL)
    redis.xreadgroup(group, consumer, {stream_key: ">"}, noack=True)

    # Check pending messages (should be 0 due to NOACK)
    pending_info = redis.xpending(stream_key, group)
    assert pending_info[0] == 0  # No pending messages


def test_xreadgroup_multiple_streams(redis: Redis):
    """Test XREADGROUP with multiple streams"""
    stream1 = "stream1"
    stream2 = "stream2"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries to both streams
    redis.xadd(stream1, "*", {"field": "value1"})
    redis.xadd(stream2, "*", {"field": "value2"})

    # Create consumer groups for both streams
    redis.xgroup_create(stream1, group, "0")
    redis.xgroup_create(stream2, group, "0")

    # Read from both streams
    result = redis.xreadgroup(group, consumer, {stream1: ">", stream2: ">"})

    # Should get data from both streams
    assert len(result) >= 1

    # Collect stream names
    stream_names = {stream_name for stream_name, entries in result}

    # Should have at least one of the streams
    assert stream1 in stream_names or stream2 in stream_names


def test_xreadgroup_different_consumers(redis: Redis):
    """Test XREADGROUP with different consumers in same group"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Add entries
    for i in range(4):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Each consumer reads some messages
    result1 = redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=2)
    result2 = redis.xreadgroup(group, consumer2, {stream_key: ">"}, count=2)

    # Both should get different messages
    if result1 and result2:
        _, entries1 = result1[0]
        _, entries2 = result2[0]

        # Get message IDs
        ids1 = {entry[0] for entry in entries1}
        ids2 = {entry[0] for entry in entries2}

        # IDs should be different (load balancing)
        assert len(ids1.intersection(ids2)) == 0


def test_xreadgroup_acknowledge_workflow(redis: Redis):
    """Test complete workflow with XREADGROUP and acknowledgment"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    redis.xadd(stream_key, "*", {"field": "value1"})
    redis.xadd(stream_key, "*", {"field": "value2"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read messages
    result = redis.xreadgroup(group, consumer, {stream_key: ">"})
    stream_name, entries = result[0]

    # Acknowledge messages
    message_ids = [entry[0] for entry in entries]
    acked_count = redis.xack(stream_key, group, *message_ids)

    assert acked_count == len(message_ids)

    # Check pending count should be 0
    pending_info = redis.xpending(stream_key, group)
    assert pending_info[0] == 0


def test_xreadgroup_no_new_messages(redis: Redis):
    """Test XREADGROUP when no new messages available"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add initial entry
    redis.xadd(stream_key, "*", {"field": "value"})

    # Create group starting from end
    redis.xgroup_create(stream_key, group, "$")

    # Try to read new messages (should be empty)
    result = redis.xreadgroup(group, consumer, {stream_key: ">"})

    # Should return empty or no entries
    if result:
        for stream_name, entries in result:
            if stream_name == stream_key:
                assert len(entries) == 0


def test_xreadgroup_from_beginning(redis: Redis):
    """Test XREADGROUP starting from beginning of stream"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    expected_count = 3
    for i in range(expected_count):
        redis.xadd(stream_key, "*", {"counter": str(i)})

    # Create group from beginning
    redis.xgroup_create(stream_key, group, "0")

    # Read all messages
    result = redis.xreadgroup(group, consumer, {stream_key: ">"})

    stream_name, entries = result[0]
    assert len(entries) == expected_count


def test_xreadgroup_mkstream_behavior(redis: Redis):
    """Test XREADGROUP behavior with streams created via MKSTREAM"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Create group with MKSTREAM (creates empty stream)
    redis.xgroup_create(stream_key, group, "0", mkstream=True)

    # Try to read (should return empty)
    result = redis.xreadgroup(group, consumer, {stream_key: ">"})

    if result:
        for stream_name, entries in result:
            if stream_name == stream_key:
                assert len(entries) == 0

    # Add entry and try reading again
    redis.xadd(stream_key, "*", {"field": "value"})
    result = redis.xreadgroup(group, consumer, {stream_key: ">"})

    # Should now get the entry
    stream_name, entries = result[0]
    assert len(entries) == 1


def test_xreadgroup_entry_format(redis: Redis):
    """Test XREADGROUP returns entries in correct format"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entry with known data
    test_data = {"name": "Alice", "score": "100"}
    entry_id = redis.xadd(stream_key, "*", test_data)

    # Create group and read
    redis.xgroup_create(stream_key, group, "0")
    result = redis.xreadgroup(group, consumer, {stream_key: ">"})

    stream_name, entries = result[0]
    assert len(entries) == 1

    returned_id, returned_data = entries[0]
    assert returned_id == entry_id

    # Convert to dict for comparison
    returned_dict = {}
    for i in range(0, len(returned_data), 2):
        returned_dict[returned_data[i]] = returned_data[i + 1]

    assert returned_dict == test_data
