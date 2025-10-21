import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xrevrange_basic(redis: Redis):
    """Test basic XREVRANGE functionality"""
    key = "test_stream"

    # Add entries in order
    redis.xadd(key, "*", {"name": "Virginia", "surname": "Woolf"})
    redis.xadd(key, "*", {"name": "Jane", "surname": "Austen"})
    redis.xadd(key, "*", {"name": "Toni", "surname": "Morrison"})
    redis.xadd(key, "*", {"name": "Agatha", "surname": "Christie"})
    redis.xadd(key, "*", {"name": "Ngozi", "surname": "Adichie"})

    # Get entries in reverse order
    result = redis.xrevrange(key, "+", "-")

    assert len(result) == 5

    # First entry should be the last one added (Ngozi Adichie)
    first_entry_id, first_entry_data = result[0]

    # Convert to dict for easier comparison
    first_entry_dict = {}
    for i in range(0, len(first_entry_data), 2):
        first_entry_dict[first_entry_data[i]] = first_entry_data[i + 1]

    assert first_entry_dict["name"] == "Ngozi"
    assert first_entry_dict["surname"] == "Adichie"


def test_xrevrange_with_count_limit(redis: Redis):
    """Test XREVRANGE with COUNT limit"""
    key = "test_stream"

    # Add entries
    redis.xadd(key, "*", {"name": "Virginia", "surname": "Woolf"})
    redis.xadd(key, "*", {"name": "Jane", "surname": "Austen"})
    redis.xadd(key, "*", {"name": "Toni", "surname": "Morrison"})
    redis.xadd(key, "*", {"name": "Agatha", "surname": "Christie"})
    redis.xadd(key, "*", {"name": "Ngozi", "surname": "Adichie"})

    # Get only last 2 entries
    result = redis.xrevrange(key, "+", "-", count=2)

    assert len(result) == 2

    # Should get the two most recent entries in reverse order
    entry1_dict = {}
    entry1_data = result[0][1]
    for i in range(0, len(entry1_data), 2):
        entry1_dict[entry1_data[i]] = entry1_data[i + 1]

    entry2_dict = {}
    entry2_data = result[1][1]
    for i in range(0, len(entry2_data), 2):
        entry2_dict[entry2_data[i]] = entry2_data[i + 1]

    # First should be Ngozi (most recent)
    assert entry1_dict["name"] == "Ngozi"
    assert entry1_dict["surname"] == "Adichie"

    # Second should be Agatha (second most recent)
    assert entry2_dict["name"] == "Agatha"
    assert entry2_dict["surname"] == "Christie"


def test_xrevrange_empty_stream(redis: Redis):
    """Test XREVRANGE on empty stream"""
    result = redis.xrevrange("nonexistent_stream", "+", "-")
    assert result == []


def test_xrevrange_single_entry(redis: Redis):
    """Test XREVRANGE with single entry"""
    key = "test_stream"

    # Add single entry
    redis.xadd(key, "*", {"field": "value"})

    # Get in reverse
    result = redis.xrevrange(key, "+", "-")

    assert len(result) == 1
    entry_id, entry_data = result[0]

    # Convert to dict
    entry_dict = {}
    for i in range(0, len(entry_data), 2):
        entry_dict[entry_data[i]] = entry_data[i + 1]

    assert entry_dict["field"] == "value"


def test_xrevrange_specific_range(redis: Redis):
    """Test XREVRANGE with specific ID range"""
    key = "test_stream"

    # Add entries with known IDs
    id1 = "1000000000000-0"
    id2 = "2000000000000-0"
    id3 = "3000000000000-0"
    id4 = "4000000000000-0"

    redis.xadd(key, id1, {"field": "value1"})
    redis.xadd(key, id2, {"field": "value2"})
    redis.xadd(key, id3, {"field": "value3"})
    redis.xadd(key, id4, {"field": "value4"})

    # Get range from id3 to id1 (reverse order)
    result = redis.xrevrange(key, id3, id1)

    # Should get id3, id2, id1 in that order
    returned_ids = [entry[0] for entry in result]

    assert id3 in returned_ids
    assert id2 in returned_ids
    assert id1 in returned_ids
    assert id4 not in returned_ids  # Outside range

    # First returned should be id3 (highest in range)
    assert returned_ids[0] == id3


def test_xrevrange_reverse_order_verification(redis: Redis):
    """Test that XREVRANGE returns entries in correct reverse chronological order"""
    key = "test_stream"

    # Add entries with sequential data
    entry_ids = []
    for i in range(5):
        entry_id = redis.xadd(key, "*", {"order": str(i)})
        entry_ids.append(entry_id)

    # Get in reverse order
    result = redis.xrevrange(key, "+", "-")

    # Should be in reverse chronological order
    returned_ids = [entry[0] for entry in result]

    # Compare with expected reverse order
    expected_reverse = list(reversed(entry_ids))
    assert returned_ids == expected_reverse

    # Verify data is also in reverse order
    for i, (entry_id, entry_data) in enumerate(result):
        entry_dict = {}
        for j in range(0, len(entry_data), 2):
            entry_dict[entry_data[j]] = entry_data[j + 1]

        # Should be in reverse order (4, 3, 2, 1, 0)
        expected_order = str(4 - i)
        assert entry_dict["order"] == expected_order


def test_xrevrange_compared_to_xrange(redis: Redis):
    """Test XREVRANGE returns same entries as XRANGE but in reverse order"""
    key = "test_stream"

    # Add multiple entries
    for i in range(10):
        redis.xadd(key, "*", {"index": str(i)})

    # Get with XRANGE (normal order)
    forward_result = redis.xrange(key, "-", "+")

    # Get with XREVRANGE (reverse order)
    reverse_result = redis.xrevrange(key, "+", "-")

    # Should have same number of entries
    assert len(forward_result) == len(reverse_result)

    # IDs should be the same but in reverse order
    forward_ids = [entry[0] for entry in forward_result]
    reverse_ids = [entry[0] for entry in reverse_result]

    assert forward_ids == list(reversed(reverse_ids))


def test_xrevrange_after_deletions(redis: Redis):
    """Test XREVRANGE after some entries are deleted"""
    key = "test_stream"

    # Add entries
    ids = []
    for i in range(5):
        entry_id = redis.xadd(key, "*", {"field": f"value{i}"})
        ids.append(entry_id)

    # Delete middle entries
    redis.xdel(key, ids[1], ids[3])

    # Get remaining in reverse order
    result = redis.xrevrange(key, "+", "-")

    # Should only have entries 0, 2, 4 (in reverse order: 4, 2, 0)
    assert len(result) == 3

    returned_ids = [entry[0] for entry in result]
    assert ids[4] in returned_ids  # Most recent remaining
    assert ids[2] in returned_ids  # Middle remaining
    assert ids[0] in returned_ids  # Oldest remaining

    # Deleted entries should not be present
    assert ids[1] not in returned_ids
    assert ids[3] not in returned_ids

    # Order should be 4, 2, 0
    assert returned_ids[0] == ids[4]  # Most recent first


def test_xrevrange_boundary_conditions(redis: Redis):
    """Test XREVRANGE boundary conditions"""
    key = "test_stream"

    # Add single entry
    entry_id = redis.xadd(key, "*", {"field": "value"})

    # Test exact boundaries
    result = redis.xrevrange(key, entry_id, entry_id)
    assert len(result) == 1
    assert result[0][0] == entry_id

    # Test inclusive boundaries
    result = redis.xrevrange(key, "+", entry_id)
    assert len(result) == 1

    result = redis.xrevrange(key, entry_id, "-")
    assert len(result) == 1


def test_xrevrange_with_trimmed_stream(redis: Redis):
    """Test XREVRANGE on a stream that has been trimmed"""
    key = "test_stream"

    # Add many entries
    for i in range(20):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Trim to keep only recent entries
    redis.xtrim(key, maxlen=5, approximate=False)

    # Get remaining in reverse order
    result = redis.xrevrange(key, "+", "-")

    # Should have at most 5 entries
    assert len(result) <= 5

    # Should be in reverse chronological order
    if len(result) > 1:
        # Compare timestamps to ensure reverse order
        for i in range(len(result) - 1):
            current_id = result[i][0]
            next_id = result[i + 1][0]

            current_timestamp = int(current_id.split("-")[0])
            next_timestamp = int(next_id.split("-")[0])

            assert current_timestamp >= next_timestamp


def test_xrevrange_large_count(redis: Redis):
    """Test XREVRANGE with count larger than stream size"""
    key = "test_stream"

    # Add few entries
    for i in range(3):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Request more than available
    result = redis.xrevrange(key, "+", "-", count=10)

    # Should return all available entries
    assert len(result) == 3
