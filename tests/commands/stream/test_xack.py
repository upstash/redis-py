import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xack_acknowledge_messages(redis: Redis):
    """Test acknowledging messages successfully"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add some messages to the stream
    id1 = redis.xadd(stream_key, "*", {"field1": "value1"})
    id2 = redis.xadd(stream_key, "*", {"field2": "value2"})

    # Create a consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read messages with the consumer group (makes them pending)
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=2)

    # Acknowledge the messages
    result = redis.xack(stream_key, group, id1, id2)
    assert result == 2


def test_xack_acknowledge_single_message(redis: Redis):
    """Test acknowledging a single message"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add a message
    message_id = redis.xadd(stream_key, "*", {"field": "value"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read the message
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    # Acknowledge it
    result = redis.xack(stream_key, group, message_id)
    assert result == 1


def test_xack_re_acknowledge_returns_zero(redis: Redis):
    """Test that re-acknowledging a message returns 0"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add a message
    message_id = redis.xadd(stream_key, "*", {"field": "value"})

    # Create consumer group with MKSTREAM
    redis.xgroup_create(stream_key, group, "0", mkstream=True)

    # Read the message
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    # Acknowledge once
    result1 = redis.xack(stream_key, group, message_id)
    assert result1 == 1

    # Try to acknowledge again
    result2 = redis.xack(stream_key, group, message_id)
    assert result2 == 0


def test_xack_nonexistent_message(redis: Redis):
    """Test acknowledging non-existent messages returns 0"""
    stream_key = "test_stream"
    group = "test_group"

    # Add a message and create group
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")

    # Try to acknowledge a non-existent message
    result = redis.xack(stream_key, group, "9999999999999-0")
    assert result == 0


def test_xack_multiple_messages(redis: Redis):
    """Test acknowledging multiple messages at once"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add multiple messages
    ids = []
    for i in range(5):
        message_id = redis.xadd(stream_key, "*", {"field": f"value{i}"})
        ids.append(message_id)

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read all messages
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=5)

    # Acknowledge all messages
    result = redis.xack(stream_key, group, *ids)
    assert result == 5


def test_xack_partial_acknowledge(redis: Redis):
    """Test acknowledging some messages out of many"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add multiple messages
    ids = []
    for i in range(5):
        message_id = redis.xadd(stream_key, "*", {"field": f"value{i}"})
        ids.append(message_id)

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read all messages
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=5)

    # Acknowledge only first 3 messages
    result = redis.xack(stream_key, group, ids[0], ids[1], ids[2])
    assert result == 3

    # Check that 2 messages are still pending
    pending_info = redis.xpending(stream_key, group)
    assert pending_info[0] == 2  # 2 messages still pending


def test_xack_wrong_group_name(redis: Redis):
    """Test acknowledging with wrong group name"""
    stream_key = "test_stream"
    group = "test_group"
    wrong_group = "wrong_group"
    consumer = "test_consumer"

    # Add a message
    message_id = redis.xadd(stream_key, "*", {"field": "value"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read the message
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    response = redis.xack(stream_key, wrong_group, message_id)
    assert response == 0
