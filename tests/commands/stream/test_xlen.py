import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xlen_basic(redis: Redis):
    """Test basic XLEN functionality"""
    key = "test_stream"

    # Add some entries
    redis.xadd(key, "*", {"name": "Jane", "surname": "Austen"})
    redis.xadd(key, "*", {"name": "Toni", "surname": "Morrison"})
    redis.xadd(key, "*", {"name": "Hezarfen", "surname": "----"})

    # Check length
    result = redis.xlen(key)
    assert result == 3


def test_xlen_empty_stream(redis: Redis):
    """Test XLEN on empty stream"""
    key = "test_stream"

    # Create empty stream
    redis.xadd(key, "*", {"field": "value"})
    redis.xdel(key, redis.xrange(key)[0][0])

    # Check length
    result = redis.xlen(key)
    assert result == 0


def test_xlen_nonexistent_stream(redis: Redis):
    """Test XLEN on non-existent stream returns 0"""
    result = redis.xlen("nonexistent_stream")
    assert result == 0


def test_xlen_after_operations(redis: Redis):
    """Test XLEN after various stream operations"""
    key = "test_stream"

    # Initially 0
    assert redis.xlen(key) == 0

    # Add entries
    id1 = redis.xadd(key, "*", {"field": "value1"})
    assert redis.xlen(key) == 1

    id2 = redis.xadd(key, "*", {"field": "value2"})
    assert redis.xlen(key) == 2

    id3 = redis.xadd(key, "*", {"field": "value3"})
    assert redis.xlen(key) == 3

    # Delete an entry
    redis.xdel(key, id2)
    assert redis.xlen(key) == 2

    # Delete all entries
    redis.xdel(key, id1, id3)
    assert redis.xlen(key) == 0


def test_xlen_with_trimming(redis: Redis):
    """Test XLEN with stream trimming"""
    key = "test_stream"

    # Add many entries
    for i in range(10):
        redis.xadd(key, "*", {"field": f"value{i}"})

    assert redis.xlen(key) == 10

    # Add entry with trimming
    redis.xadd(key, "*", {"field": "final"}, maxlen=5, approximate_trim=False)

    # Length should be at most 5
    length = redis.xlen(key)
    assert length <= 5


def test_xlen_large_stream(redis: Redis):
    """Test XLEN with a larger number of entries"""
    key = "test_stream"

    # Add many entries
    entry_count = 100
    for i in range(entry_count):
        redis.xadd(key, "*", {"index": str(i), "data": f"data_{i}"})

    # Check length
    result = redis.xlen(key)
    assert result == entry_count


def test_xlen_with_consumer_groups(redis: Redis):
    """Test that XLEN is not affected by consumer groups"""
    key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    for i in range(5):
        redis.xadd(key, "*", {"field": f"value{i}"})

    initial_length = redis.xlen(key)
    assert initial_length == 5

    # Create consumer group and read messages
    redis.xgroup_create(key, group, "0")
    redis.xreadgroup(group, consumer, {key: ">"}, count=3)

    # Length should remain the same
    length_after_read = redis.xlen(key)
    assert length_after_read == initial_length == 5

    # Acknowledge some messages
    entries = redis.xrange(key, "-", "+", count=3)
    message_ids = [entry[0] for entry in entries]
    redis.xack(key, group, *message_ids)

    # Length should still be the same
    length_after_ack = redis.xlen(key)
    assert length_after_ack == initial_length == 5


def test_xlen_incremental(redis: Redis):
    """Test XLEN incrementally as entries are added"""
    key = "test_stream"

    expected_length = 0
    assert redis.xlen(key) == expected_length

    # Add entries one by one and check length
    for i in range(20):
        redis.xadd(key, "*", {"counter": str(i)})
        expected_length += 1
        assert redis.xlen(key) == expected_length


def test_xlen_multiple_streams(redis: Redis):
    """Test XLEN on multiple different streams"""
    stream1 = "stream1"
    stream2 = "stream2"
    stream3 = "stream3"

    # Add different numbers of entries to each stream
    for i in range(3):
        redis.xadd(stream1, "*", {"field": f"value{i}"})

    for i in range(7):
        redis.xadd(stream2, "*", {"field": f"value{i}"})

    # stream3 remains empty

    # Check lengths
    assert redis.xlen(stream1) == 3
    assert redis.xlen(stream2) == 7
    assert redis.xlen(stream3) == 0
    assert redis.xlen("nonexistent") == 0
