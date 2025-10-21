import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xgroup_create_basic(redis: Redis):
    """Test creating a consumer group"""
    key = "test_stream"
    group = "test_group"

    # Create stream first
    redis.xadd(key, "*", {"field": "value"})

    # Create consumer group
    result = redis.xgroup_create(key, group, "$")
    assert result == "OK"


def test_xgroup_create_with_mkstream(redis: Redis):
    """Test creating a consumer group with MKSTREAM"""
    key = "test_stream"
    group = "test_group"

    # Create group on non-existent stream using MKSTREAM
    result = redis.xgroup_create(key, group, "$", mkstream=True)
    assert result == "OK"

    # Verify stream exists now
    length = redis.xlen(key)
    assert length == 0  # Empty stream


def test_xgroup_create_without_mkstream_should_fail(redis: Redis):
    """Test that creating group on non-existent stream fails without MKSTREAM"""
    key = "nonexistent_stream"
    group = "test_group"

    # This should raise an exception
    with pytest.raises(Exception):
        redis.xgroup_create(key, group, "$")


def test_xgroup_createconsumer(redis: Redis):
    """Test creating a consumer in a group"""
    key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Create stream and group first
    redis.xadd(key, "*", {"field": "value"})
    redis.xgroup_create(key, group, "0")

    # Create consumer
    result = redis.xgroup_createconsumer(key, group, consumer)
    assert result == 1  # Consumer was created


def test_xgroup_delconsumer(redis: Redis):
    """Test deleting a consumer from a group"""
    key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Setup stream, group, and consumer
    redis.xadd(key, "*", {"field": "value1"})
    redis.xadd(key, "*", {"field": "value2"})
    redis.xgroup_create(key, group, "0")
    redis.xgroup_createconsumer(key, group, consumer)

    # Read some messages to create pending messages
    redis.xreadgroup(group, consumer, {key: ">"}, count=2)

    # Delete consumer (should return number of pending messages)
    result = redis.xgroup_delconsumer(key, group, consumer)
    assert result >= 0  # Should return number of pending messages deleted


def test_xgroup_delconsumer_nonexistent(redis: Redis):
    """Test deleting non-existent consumer returns 0"""
    key = "test_stream"
    group = "test_group"

    # Create stream and group
    redis.xadd(key, "*", {"field": "value"})
    redis.xgroup_create(key, group, "0")

    # Try to delete non-existent consumer
    result = redis.xgroup_delconsumer(key, group, "nonexistent_consumer")
    assert result == 0


def test_xgroup_destroy(redis: Redis):
    """Test destroying a consumer group"""
    key = "test_stream"
    group = "test_group"

    # Create stream and group
    redis.xadd(key, "*", {"field": "value"})
    redis.xgroup_create(key, group, "0")

    # Destroy the group
    result = redis.xgroup_destroy(key, group)
    assert result == 1  # Group was destroyed


def test_xgroup_destroy_nonexistent(redis: Redis):
    """Test destroying non-existent group returns 0"""
    key = "test_stream"

    # Create stream only
    redis.xadd(key, "*", {"field": "value"})

    # Try to destroy non-existent group
    result = redis.xgroup_destroy(key, "nonexistent_group")
    assert result == 0


def test_xgroup_destroy_after_use(redis: Redis):
    """Test destroying group after it has been used"""
    key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Setup and use the group
    redis.xadd(key, "*", {"field": "value1"})
    redis.xadd(key, "*", {"field": "value2"})
    redis.xgroup_create(key, group, "0")
    redis.xreadgroup(group, consumer, {key: ">"}, count=2)

    # Destroy the group
    result = redis.xgroup_destroy(key, group)
    assert result == 1


def test_xgroup_setid(redis: Redis):
    """Test setting group's last delivered ID"""
    key = "test_stream"
    group = "test_group"

    # Create stream and group
    redis.xadd(key, "*", {"field": "value1"})
    redis.xadd(key, "*", {"field": "value2"})
    redis.xgroup_create(key, group, "0")

    # Set ID to current end
    result = redis.xgroup_setid(key, group, "$")
    assert result == "OK"


def test_xgroup_setid_with_entries_read(redis: Redis):
    """Test setting group ID with ENTRIESREAD"""
    key = "test_stream"
    group = "test_group"

    # Create stream and group
    for i in range(5):
        redis.xadd(key, "*", {"field": f"value{i}"})
    redis.xgroup_create(key, group, "0")

    # Set ID with entries_read
    result = redis.xgroup_setid(key, group, "$", entries_read=3)
    assert result == "OK"


def test_xgroup_workflow_complete(redis: Redis):
    """Test complete workflow with multiple group operations"""
    key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Add some data
    for i in range(3):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Create group
    redis.xgroup_create(key, group, "0")

    # Create consumers
    redis.xgroup_createconsumer(key, group, consumer1)
    redis.xgroup_createconsumer(key, group, consumer2)

    # Use the group
    redis.xreadgroup(group, consumer1, {key: ">"}, count=2)
    redis.xreadgroup(group, consumer2, {key: ">"}, count=1)

    # Clean up consumers
    redis.xgroup_delconsumer(key, group, consumer1)
    redis.xgroup_delconsumer(key, group, consumer2)

    # Destroy group
    result = redis.xgroup_destroy(key, group)
    assert result == 1
