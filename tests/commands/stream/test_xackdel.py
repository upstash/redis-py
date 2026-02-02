import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xackdel_acknowledge_and_delete_messages(redis: Redis):
    """Test acknowledging and deleting messages successfully"""
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

    # Acknowledge and delete the messages
    result = redis.xackdel(stream_key, group, id1, id2)
    assert isinstance(result, list)
    assert len(result) == 2

    # Verify messages are deleted
    length = redis.xlen(stream_key)
    assert length == 0


def test_xackdel_single_message(redis: Redis):
    """Test acknowledging and deleting a single message"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add a message
    message_id = redis.xadd(stream_key, "*", {"field": "value"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read the message
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    # Acknowledge and delete it
    result = redis.xackdel(stream_key, group, message_id)
    assert isinstance(result, list)
    assert len(result) == 1

    # Verify message is deleted
    length = redis.xlen(stream_key)
    assert length == 0


def test_xackdel_with_keepref_option(redis: Redis):
    """Test XACKDEL with KEEPREF option"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add messages
    id1 = redis.xadd(stream_key, "*", {"field": "value1"})
    redis.xadd(stream_key, "*", {"field": "value2"})  # id2

    # Create consumer group and read messages
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=2)

    # Acknowledge and delete with KEEPREF option
    result = redis.xackdel(stream_key, group, id1, option="KEEPREF")
    assert isinstance(result, list)
    assert len(result) == 1


def test_xackdel_with_delref_option(redis: Redis):
    """Test XACKDEL with DELREF option"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add messages
    id1 = redis.xadd(stream_key, "*", {"field": "value1"})
    redis.xadd(stream_key, "*", {"field": "value2"})  # id2

    # Create consumer group and read messages
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=2)

    # Acknowledge and delete with DELREF option
    result = redis.xackdel(stream_key, group, id1, option="DELREF")
    assert isinstance(result, list)
    assert len(result) == 1


def test_xackdel_with_acked_option(redis: Redis):
    """Test XACKDEL with ACKED option"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add messages
    id1 = redis.xadd(stream_key, "*", {"field": "value1"})
    id2 = redis.xadd(stream_key, "*", {"field": "value2"})

    # Create consumer group and read messages
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=2)

    # First acknowledge the messages
    redis.xack(stream_key, group, id1, id2)

    # Then use XACKDEL with ACKED option
    result = redis.xackdel(stream_key, group, id1, id2, option="ACKED")
    assert isinstance(result, list)
    assert len(result) == 2


def test_xackdel_case_insensitive_option(redis: Redis):
    """Test that option is case-insensitive"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add messages
    id1 = redis.xadd(stream_key, "*", {"field": "value1"})
    id2 = redis.xadd(stream_key, "*", {"field": "value2"})

    # Create consumer group and read messages
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=2)

    # Test with lowercase option
    result1 = redis.xackdel(stream_key, group, id1, option="keepref")
    assert isinstance(result1, list)

    # Test with uppercase option
    result2 = redis.xackdel(stream_key, group, id2, option="DELREF")
    assert isinstance(result2, list)


def test_xackdel_nonexistent_message(redis: Redis):
    """Test acknowledging and deleting non-existent messages"""
    stream_key = "test_stream"
    group = "test_group"

    # Add a message and create group
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")

    # Try to acknowledge and delete a non-existent message
    result = redis.xackdel(stream_key, group, "9999999999999-0")
    assert isinstance(result, list)
    assert len(result) == 1


def test_xackdel_multiple_messages(redis: Redis):
    """Test acknowledging and deleting multiple messages at once"""
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

    # Acknowledge and delete all messages
    result = redis.xackdel(stream_key, group, *ids)
    assert isinstance(result, list)
    assert len(result) == 5

    # Verify all messages are deleted
    length = redis.xlen(stream_key)
    assert length == 0


def test_xackdel_partial_messages(redis: Redis):
    """Test acknowledging and deleting some messages out of many"""
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

    # Acknowledge and delete only first 3 messages
    result = redis.xackdel(stream_key, group, ids[0], ids[1], ids[2])
    assert isinstance(result, list)
    assert len(result) == 3

    # Verify 2 messages still remain
    length = redis.xlen(stream_key)
    assert length == 2


def test_xackdel_requires_at_least_one_id(redis: Redis):
    """Test that XACKDEL requires at least one ID"""
    stream_key = "test_stream"
    group = "test_group"

    with pytest.raises(Exception, match="requires at least one ID"):
        redis.xackdel(stream_key, group)


def test_xackdel_wrong_group_name(redis: Redis):
    """Test acknowledging and deleting with wrong group name"""
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

    # Try with wrong group (should still return a result)
    result = redis.xackdel(stream_key, wrong_group, message_id)
    assert isinstance(result, list)
    assert len(result) == 1


def test_xackdel_unread_messages(redis: Redis):
    """Test XACKDEL on messages that haven't been read yet"""
    stream_key = "test_stream"
    group = "test_group"

    # Add messages but don't read them
    id1 = redis.xadd(stream_key, "*", {"field": "value1"})
    id2 = redis.xadd(stream_key, "*", {"field": "value2"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Try to acknowledge and delete without reading
    result = redis.xackdel(stream_key, group, id1, id2)
    assert isinstance(result, list)
    assert len(result) == 2


def test_xackdel_already_acknowledged(redis: Redis):
    """Test XACKDEL on already acknowledged messages"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add a message
    message_id = redis.xadd(stream_key, "*", {"field": "value"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Read the message
    redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    # Acknowledge first
    redis.xack(stream_key, group, message_id)

    # Then try XACKDEL
    result = redis.xackdel(stream_key, group, message_id)
    assert isinstance(result, list)
    assert len(result) == 1

    # Message should be deleted
    length = redis.xlen(stream_key)
    assert length == 0
