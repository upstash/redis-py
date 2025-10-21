import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xrange_basic(redis: Redis):
    """Test basic XRANGE functionality"""
    key = "test_stream"

    # Add entries
    field1, field2 = "field1", "field2"
    value1, value2 = "test_value1", "test_value2"

    redis.xadd(key, "*", {field1: value1, field2: value2})

    # Get all entries
    result = redis.xrange(key, "-", "+")
    assert len(result) == 1

    # Check structure [id, [field1, value1, field2, value2]]
    entry_id, entry_data = result[0]
    assert isinstance(entry_id, str)
    assert isinstance(entry_data, list)

    # Convert to dict for easier testing
    entry_dict = {}
    for i in range(0, len(entry_data), 2):
        entry_dict[entry_data[i]] = entry_data[i + 1]

    assert entry_dict[field1] == value1
    assert entry_dict[field2] == value2


def test_xrange_with_count_limit(redis: Redis):
    """Test XRANGE with COUNT limit"""
    key = "test_stream"

    # Add multiple entries
    for i in range(5):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Get only first 2 entries
    result = redis.xrange(key, "-", "+", count=2)
    assert len(result) == 2

    # Verify they are the first entries (oldest)
    for i, (entry_id, entry_data) in enumerate(result):
        entry_dict = {}
        for j in range(0, len(entry_data), 2):
            entry_dict[entry_data[j]] = entry_data[j + 1]
        assert entry_dict["field"] == f"value{i}"


def test_xrange_empty_stream(redis: Redis):
    """Test XRANGE on empty stream"""
    result = redis.xrange("nonexistent_stream", "-", "+")
    assert result == []


def test_xrange_specific_range(redis: Redis):
    """Test XRANGE with specific ID range"""
    key = "test_stream"

    # Add entries with specific IDs
    id1 = "1000000000000-0"
    id2 = "2000000000000-0"
    id3 = "3000000000000-0"

    redis.xadd(key, id1, {"field": "value1"})
    redis.xadd(key, id2, {"field": "value2"})
    redis.xadd(key, id3, {"field": "value3"})

    # Get range from id1 to id2
    result = redis.xrange(key, id1, id2)
    assert len(result) == 2

    returned_ids = [entry[0] for entry in result]
    assert id1 in returned_ids
    assert id2 in returned_ids
    assert id3 not in returned_ids


def test_xrange_single_entry(redis: Redis):
    """Test XRANGE targeting single entry"""
    key = "test_stream"

    # Add entries
    redis.xadd(key, "*", {"field": "value1"})
    id2 = redis.xadd(key, "*", {"field": "value2"})
    redis.xadd(key, "*", {"field": "value3"})

    # Get only the middle entry
    result = redis.xrange(key, id2, id2)
    assert len(result) == 1
    assert result[0][0] == id2


def test_xrange_many_fields(redis: Redis):
    """Test XRANGE with entries having many fields"""
    key = "test_stream"

    # Create entry with multiple fields
    fields = {}
    for i in range(10):
        fields[f"field_{i}"] = f"value_{i}"

    entry_id = redis.xadd(key, "*", fields)

    # Retrieve and verify
    result = redis.xrange(key, "-", "+")
    assert len(result) == 1

    returned_id, returned_data = result[0]
    assert returned_id == entry_id

    # Convert to dict
    returned_dict = {}
    for i in range(0, len(returned_data), 2):
        returned_dict[returned_data[i]] = returned_data[i + 1]

    assert returned_dict == fields


def test_xrange_progressive_entries(redis: Redis):
    """Test XRANGE as entries are progressively added"""
    key = "test_stream"

    # Add entries progressively and test range each time
    for i in range(1, 6):
        redis.xadd(key, "*", {"counter": str(i)})

        result = redis.xrange(key, "-", "+")
        assert len(result) == i

        # Verify all entries are present
        counters = []
        for entry_id, entry_data in result:
            entry_dict = {}
            for j in range(0, len(entry_data), 2):
                entry_dict[entry_data[j]] = entry_data[j + 1]
            counters.append(int(entry_dict["counter"]))

        assert counters == list(range(1, i + 1))


def test_xrange_after_deletion(redis: Redis):
    """Test XRANGE after some entries are deleted"""
    key = "test_stream"

    # Add entries
    ids = []
    for i in range(5):
        entry_id = redis.xadd(key, "*", {"field": f"value{i}"})
        ids.append(entry_id)

    # Delete middle entries
    redis.xdel(key, ids[1], ids[3])

    # Range should return remaining entries
    result = redis.xrange(key, "-", "+")
    assert len(result) == 3

    returned_ids = [entry[0] for entry in result]
    assert ids[0] in returned_ids
    assert ids[2] in returned_ids
    assert ids[4] in returned_ids
    assert ids[1] not in returned_ids
    assert ids[3] not in returned_ids


def test_xrange_with_trimmed_stream(redis: Redis):
    """Test XRANGE with a stream that has been trimmed"""
    key = "test_stream"

    # Add many entries
    for i in range(10):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Trim stream
    redis.xtrim(key, maxlen=5, approximate=False)

    # Range should show only remaining entries
    result = redis.xrange(key, "-", "+")
    assert len(result) <= 5


def test_xrange_ordering(redis: Redis):
    """Test that XRANGE returns entries in correct order"""
    key = "test_stream"

    # Add entries
    ids = []
    for i in range(5):
        entry_id = redis.xadd(key, "*", {"order": str(i)})
        ids.append(entry_id)

    # Get all entries
    result = redis.xrange(key, "-", "+")

    # Verify order is maintained (chronological)
    returned_ids = [entry[0] for entry in result]
    assert returned_ids == ids


def test_xrange_boundary_conditions(redis: Redis):
    """Test XRANGE boundary conditions"""
    key = "test_stream"

    # Add single entry
    entry_id = redis.xadd(key, "*", {"field": "value"})

    # Test exact boundaries
    result = redis.xrange(key, entry_id, entry_id)
    assert len(result) == 1
    assert result[0][0] == entry_id

    # Test exclusive boundaries (using different IDs)
    result = redis.xrange(key, "0-0", entry_id)
    assert len(result) == 1

    result = redis.xrange(key, entry_id, "+")
    assert len(result) == 1
