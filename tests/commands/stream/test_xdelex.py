import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xdelex_single_entry(redis: Redis):
    """Test extended delete of a single entry from stream"""
    key = "test_stream"

    # Add some entries
    id1 = redis.xadd(key, "*", {"name": "Jane", "surname": "Austen"})
    id2 = redis.xadd(key, "*", {"name": "Toni", "surname": "Morrison"})

    # Delete one entry with XDELEX
    result = redis.xdelex(key, id2)
    assert isinstance(result, list)
    assert len(result) == 1

    # Verify only one entry remains
    entries = redis.xrange(key, "-", "+")
    assert len(entries) == 1
    assert entries[0][0] == id1


def test_xdelex_multiple_entries(redis: Redis):
    """Test extended delete of multiple entries from stream"""
    key = "test_stream"

    # Add multiple entries
    id1 = redis.xadd(key, "*", {"name": "Jane", "surname": "Austen"})
    id2 = redis.xadd(key, "*", {"name": "Toni", "surname": "Morrison"})
    id3 = redis.xadd(key, "*", {"name": "Agatha", "surname": "Christie"})
    id4 = redis.xadd(key, "*", {"name": "Ngozi", "surname": "Adichie"})

    # Delete multiple entries at once
    result = redis.xdelex(key, id1, id2, id3)
    assert isinstance(result, list)
    assert len(result) == 3

    # Verify only one entry remains
    entries = redis.xrange(key, "-", "+")
    assert len(entries) == 1
    assert entries[0][0] == id4


def test_xdelex_with_keepref_option(redis: Redis):
    """Test XDELEX with KEEPREF option"""
    key = "test_stream"

    # Add entries
    id1 = redis.xadd(key, "*", {"field": "value1"})
    redis.xadd(key, "*", {"field": "value2"})  # id2

    # Delete with KEEPREF option
    result = redis.xdelex(key, id1, option="KEEPREF")
    assert isinstance(result, list)
    assert len(result) == 1


def test_xdelex_with_delref_option(redis: Redis):
    """Test XDELEX with DELREF option"""
    key = "test_stream"

    # Add entries
    id1 = redis.xadd(key, "*", {"field": "value1"})
    redis.xadd(key, "*", {"field": "value2"})  # id2

    # Delete with DELREF option
    result = redis.xdelex(key, id1, option="DELREF")
    assert isinstance(result, list)
    assert len(result) == 1


def test_xdelex_with_acked_option(redis: Redis):
    """Test XDELEX with ACKED option"""
    key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    id1 = redis.xadd(key, "*", {"field": "value1"})
    id2 = redis.xadd(key, "*", {"field": "value2"})

    # Create consumer group and read messages
    redis.xgroup_create(key, group, "0")
    redis.xreadgroup(group, consumer, {key: ">"}, count=2)

    # Acknowledge messages
    redis.xack(key, group, id1, id2)

    # Delete with ACKED option (only deletes acknowledged messages)
    result = redis.xdelex(key, id1, id2, option="ACKED")
    assert isinstance(result, list)
    assert len(result) == 2


def test_xdelex_case_insensitive_option(redis: Redis):
    """Test that option is case-insensitive"""
    key = "test_stream"

    # Add entries
    id1 = redis.xadd(key, "*", {"field": "value1"})
    id2 = redis.xadd(key, "*", {"field": "value2"})

    # Test with lowercase option
    result1 = redis.xdelex(key, id1, option="keepref")
    assert isinstance(result1, list)

    # Test with uppercase option
    result2 = redis.xdelex(key, id2, option="DELREF")
    assert isinstance(result2, list)


def test_xdelex_nonexistent_entry(redis: Redis):
    """Test extended delete of a non-existent entry"""
    key = "test_stream"

    # Add an entry
    redis.xadd(key, "*", {"field": "value"})

    # Try to delete non-existent entry
    result = redis.xdelex(key, "9999999999999-0")
    assert isinstance(result, list)
    assert len(result) == 1


def test_xdelex_from_nonexistent_stream(redis: Redis):
    """Test extended delete from non-existent stream"""
    result = redis.xdelex("nonexistent_stream", "1234567890-0")
    assert isinstance(result, list)
    assert len(result) == 1


def test_xdelex_all_entries(redis: Redis):
    """Test extended delete of all entries from a stream"""
    key = "test_stream"

    # Add several entries
    ids = []
    for i in range(5):
        entry_id = redis.xadd(key, "*", {"field": f"value{i}"})
        ids.append(entry_id)

    # Delete all entries
    result = redis.xdelex(key, *ids)
    assert isinstance(result, list)
    assert len(result) == 5

    # Stream should still exist but be empty
    length = redis.xlen(key)
    assert length == 0


def test_xdelex_requires_at_least_one_id(redis: Redis):
    """Test that XDELEX requires at least one ID"""
    key = "test_stream"

    with pytest.raises(Exception, match="requires at least one ID"):
        redis.xdelex(key)


def test_xdelex_with_consumer_groups(redis: Redis):
    """Test that XDELEX works even when stream has consumer groups"""
    key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    id1 = redis.xadd(key, "*", {"field": "value1"})
    id2 = redis.xadd(key, "*", {"field": "value2"})

    # Create consumer group
    redis.xgroup_create(key, group, "0")

    # Read some messages
    redis.xreadgroup(group, consumer, {key: ">"}, count=2)

    # Delete entries with XDELEX
    result = redis.xdelex(key, id1, id2)
    assert isinstance(result, list)
    assert len(result) == 2

    # Stream should be empty
    length = redis.xlen(key)
    assert length == 0


def test_xdelex_partial_success(redis: Redis):
    """Test extended delete with mix of existing and non-existing entries"""
    key = "test_stream"

    # Add some entries
    id1 = redis.xadd(key, "*", {"field": "value1"})
    id2 = redis.xadd(key, "*", {"field": "value2"})

    # Try to delete existing and non-existing entries
    result = redis.xdelex(key, id1, "9999999999999-0", id2, "8888888888888-0")
    assert isinstance(result, list)
    assert len(result) == 4

    # Stream should be empty now
    length = redis.xlen(key)
    assert length == 0

