import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xinfo_groups_empty_stream(redis: Redis):
    """Test XINFO GROUPS on stream with no groups"""
    stream_key = "test_stream"

    # Add entries to stream
    for i in range(3):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Check groups (should be empty)
    result = redis.xinfo_groups(stream_key)
    assert result == []


def test_xinfo_groups_with_groups(redis: Redis):
    """Test XINFO GROUPS on stream with consumer groups"""
    stream_key = "test_stream"
    group = "test_group"

    # Add entries
    for i in range(3):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create consumer group
    redis.xgroup_create(stream_key, group, "0")

    # Get group info
    result = redis.xinfo_groups(stream_key)
    assert len(result) == 1

    # Result should be list of group info arrays
    group_info = result[0]
    assert isinstance(group_info, list)

    # Find group name in the info (format: [key, value, key, value, ...])
    group_info_dict = {}
    for i in range(0, len(group_info), 2):
        group_info_dict[group_info[i]] = group_info[i + 1]

    assert group_info_dict.get("name") == group


def test_xinfo_consumers_empty_group(redis: Redis):
    """Test XINFO CONSUMERS on group with no consumers"""
    stream_key = "test_stream"
    group = "test_group"

    # Add entries and create group
    redis.xadd(stream_key, "*", {"field": "value"})
    redis.xgroup_create(stream_key, group, "0")

    # Check consumers (should be empty)
    result = redis.xinfo_consumers(stream_key, group)
    assert result == []


def test_xinfo_consumers_with_consumers(redis: Redis):
    """Test XINFO CONSUMERS on group with active consumers"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    for i in range(3):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create group and consumer, then read messages
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer, {stream_key: ">"})

    # Get consumer info
    result = redis.xinfo_consumers(stream_key, group)
    assert len(result) >= 1

    # Check if our consumer is in the list
    found_consumer = False
    for consumer_info in result:
        # Convert to dict
        consumer_dict = {}
        for i in range(0, len(consumer_info), 2):
            consumer_dict[consumer_info[i]] = consumer_info[i + 1]

        if consumer_dict.get("name") == consumer:
            found_consumer = True
            # Should have pending messages
            assert consumer_dict.get("pending", 0) >= 0

    assert found_consumer


def test_xinfo_nonexistent_group(redis: Redis):
    """Test XINFO CONSUMERS on non-existent group"""
    stream_key = "test_stream"
    nonexistent_group = "nonexistent_group"

    # Create stream but no group
    redis.xadd(stream_key, "*", {"field": "value"})

    response = redis.xinfo_consumers(stream_key, nonexistent_group)
    assert response == []


def test_xinfo_multiple_groups(redis: Redis):
    """Test XINFO GROUPS with multiple consumer groups"""
    stream_key = "test_stream"
    group1 = "group1"
    group2 = "group2"

    # Add entries
    redis.xadd(stream_key, "*", {"field": "value"})

    # Create multiple groups
    redis.xgroup_create(stream_key, group1, "0")
    redis.xgroup_create(stream_key, group2, "$")

    # Get groups info
    result = redis.xinfo_groups(stream_key)
    assert len(result) == 2

    # Collect group names
    group_names = set()
    for group_info in result:
        group_dict = {}
        for i in range(0, len(group_info), 2):
            group_dict[group_info[i]] = group_info[i + 1]
        group_names.add(group_dict.get("name"))

    assert group1 in group_names
    assert group2 in group_names


def test_xinfo_multiple_consumers(redis: Redis):
    """Test XINFO CONSUMERS with multiple consumers"""
    stream_key = "test_stream"
    group = "test_group"
    consumer1 = "consumer1"
    consumer2 = "consumer2"

    # Add entries
    for i in range(4):
        redis.xadd(stream_key, "*", {"field": f"value{i}"})

    # Create group and consumers
    redis.xgroup_create(stream_key, group, "0")
    redis.xreadgroup(group, consumer1, {stream_key: ">"}, count=2)
    redis.xreadgroup(group, consumer2, {stream_key: ">"}, count=2)

    # Get consumers info
    result = redis.xinfo_consumers(stream_key, group)
    assert len(result) >= 2

    # Collect consumer names
    consumer_names = set()
    for consumer_info in result:
        consumer_dict = {}
        for i in range(0, len(consumer_info), 2):
            consumer_dict[consumer_info[i]] = consumer_info[i + 1]
        consumer_names.add(consumer_dict.get("name"))

    assert consumer1 in consumer_names
    assert consumer2 in consumer_names
