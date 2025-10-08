import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xdel_single_entry(redis: Redis):
    """Test deleting a single entry from stream"""
    key = "test_stream"

    # Add some entries
    id1 = redis.xadd(key, "*", {"name": "Jane", "surname": "Austen"})
    id2 = redis.xadd(key, "*", {"name": "Toni", "surname": "Morrison"})

    # Delete one entry
    result = redis.xdel(key, id2)
    assert result == 1

    # Verify only one entry remains
    entries = redis.xrange(key, "-", "+")
    assert len(entries) == 1
    assert entries[0][0] == id1


def test_xdel_multiple_entries(redis: Redis):
    """Test deleting multiple entries from stream"""
    key = "test_stream"

    # Add multiple entries
    id1 = redis.xadd(key, "*", {"name": "Jane", "surname": "Austen"})
    id2 = redis.xadd(key, "*", {"name": "Toni", "surname": "Morrison"})
    id3 = redis.xadd(key, "*", {"name": "Agatha", "surname": "Christie"})
    id4 = redis.xadd(key, "*", {"name": "Ngozi", "surname": "Adichie"})

    # Delete multiple entries at once
    result = redis.xdel(key, id1, id2, id3)
    assert result == 3

    # Verify only one entry remains
    entries = redis.xrange(key, "-", "+")
    assert len(entries) == 1
    assert entries[0][0] == id4


def test_xdel_nonexistent_entry(redis: Redis):
    """Test deleting a non-existent entry returns 0"""
    key = "test_stream"

    # Add an entry
    redis.xadd(key, "*", {"field": "value"})

    # Try to delete non-existent entry
    result = redis.xdel(key, "9999999999999-0")
    assert result == 0


def test_xdel_from_nonexistent_stream(redis: Redis):
    """Test deleting from non-existent stream returns 0"""
    result = redis.xdel("nonexistent_stream", "1234567890-0")
    assert result == 0


def test_xdel_all_entries(redis: Redis):
    """Test deleting all entries from a stream"""
    key = "test_stream"

    # Add several entries
    ids = []
    for i in range(5):
        entry_id = redis.xadd(key, "*", {"field": f"value{i}"})
        ids.append(entry_id)

    # Delete all entries
    result = redis.xdel(key, *ids)
    assert result == 5

    # Stream should still exist but be empty
    length = redis.xlen(key)
    assert length == 0

    # Range should return empty list
    entries = redis.xrange(key, "-", "+")
    assert entries == []


def test_xdel_partial_success(redis: Redis):
    """Test deleting mix of existing and non-existing entries"""
    key = "test_stream"

    # Add some entries
    id1 = redis.xadd(key, "*", {"field": "value1"})
    id2 = redis.xadd(key, "*", {"field": "value2"})

    # Try to delete existing and non-existing entries
    result = redis.xdel(key, id1, "9999999999999-0", id2, "8888888888888-0")
    assert result == 2  # Only the 2 existing entries should be deleted

    # Stream should be empty now
    length = redis.xlen(key)
    assert length == 0


def test_xdel_empty_stream(redis: Redis):
    """Test deleting from empty stream"""
    key = "test_stream"

    # Create empty stream by adding and deleting an entry
    entry_id = redis.xadd(key, "*", {"field": "value"})
    redis.xdel(key, entry_id)

    # Try to delete from now-empty stream
    result = redis.xdel(key, "1234567890-0")
    assert result == 0


def test_xdel_with_consumer_groups(redis: Redis):
    """Test that XDEL works even when stream has consumer groups"""
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

    # Delete entries (this should work even with pending messages)
    result = redis.xdel(key, id1, id2)
    assert result == 2

    # Stream should be empty
    length = redis.xlen(key)
    assert length == 0


def test_xdel_preserves_stream_metadata(redis: Redis):
    """Test that XDEL doesn't affect stream metadata like length correctly"""
    key = "test_stream"

    # Add multiple entries
    ids = []
    for i in range(10):
        entry_id = redis.xadd(key, "*", {"field": f"value{i}"})
        ids.append(entry_id)

    initial_length = redis.xlen(key)
    assert initial_length == 10

    # Delete some entries
    deleted_count = redis.xdel(key, ids[0], ids[2], ids[4])
    assert deleted_count == 3

    # Check updated length
    new_length = redis.xlen(key)
    assert new_length == 7

    # Verify remaining entries are correct
    remaining_entries = redis.xrange(key, "-", "+")
    remaining_ids = [entry[0] for entry in remaining_entries]

    expected_remaining = [ids[1], ids[3], ids[5], ids[6], ids[7], ids[8], ids[9]]
    assert remaining_ids == expected_remaining
