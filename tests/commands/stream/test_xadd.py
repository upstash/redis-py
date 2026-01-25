import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xadd_basic(redis: Redis):
    """Test basic XADD functionality"""
    result = redis.xadd("mystream", "*", {"field1": "value1", "field2": "value2"})
    assert isinstance(result, str)
    assert len(result) > 0
    # Stream ID format should be timestamp-sequence
    assert "-" in result


def test_xadd_with_specific_id(redis: Redis):
    """Test XADD with a specific ID"""
    stream_id = "1609459200000-0"
    result = redis.xadd("mystream", stream_id, {"field": "value"})
    assert result == stream_id


def test_xadd_with_maxlen_exact(redis: Redis):
    """Test XADD with exact maxlen trimming"""
    # Add multiple entries
    for i in range(10):
        redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Add with maxlen=5 (exact)
    redis.xadd(
        "mystream", "*", {"field": "new_value"}, maxlen=5, approximate_trim=False
    )

    # Check stream length
    length = redis.xlen("mystream")
    assert length <= 5


def test_xadd_with_maxlen_approximate(redis: Redis):
    """Test XADD with approximate maxlen trimming"""
    # Add multiple entries
    for i in range(110):
        redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Add with approximate maxlen
    redis.xadd(
        "mystream", "*", {"field": "new_value"}, maxlen=30, approximate_trim=True
    )

    # Check stream length (should be around 30, but may be slightly more due to approximation)
    length = redis.xlen("mystream")
    assert length >= 100
    assert length <= 120  # Allow some variance for approximation


def test_xadd_with_nomkstream(redis: Redis):
    """Test XADD with NOMKSTREAM option"""
    # Should return None when stream doesn't exist and NOMKSTREAM is used
    result = redis.xadd("nonexistent", "*", {"field": "value"}, nomkstream=True)
    assert result is None

    # Create the stream first
    redis.xadd("mystream", "*", {"field": "value1"})

    # Now NOMKSTREAM should work
    result = redis.xadd("mystream", "*", {"field": "value2"}, nomkstream=True)
    assert isinstance(result, str)


def test_xadd_with_minid(redis: Redis):
    """Test XADD with MINID trimming"""
    # Add some entries with known IDs
    redis.xadd("mystream", "1000-0", {"field": "old1"})
    redis.xadd("mystream", "2000-0", {"field": "old2"})
    redis.xadd("mystream", "3000-0", {"field": "old3"})

    # Add new entry with MINID that should trim old entries
    redis.xadd(
        "mystream", "4000-0", {"field": "new"}, minid="2500-0", approximate_trim=False
    )

    # Check that old entries before minid are removed
    entries = redis.xrange("mystream")
    entry_ids = [entry[0] for entry in entries]

    # Should not contain entries with IDs less than 2500-0
    for entry_id in entry_ids:
        timestamp = int(entry_id.split("-")[0])
        assert timestamp >= 2500


def test_xadd_multiple_fields(redis: Redis):
    """Test XADD with multiple fields"""
    data = {"name": "John", "age": "30", "city": "New York", "score": "95.5"}

    result = redis.xadd("mystream", "*", data)
    assert isinstance(result, str)

    # Verify the data was stored correctly
    entries = redis.xrange("mystream")
    assert len(entries) == 1

    entry_id, entry_data = entries[0]
    assert entry_id == result

    # Convert list to dict for easier comparison
    entry_dict = {}
    for i in range(0, len(entry_data), 2):
        entry_dict[entry_data[i]] = entry_data[i + 1]

    assert entry_dict == data


def test_xadd_empty_data_should_fail(redis: Redis):
    """Test that XADD with empty data should fail"""
    with pytest.raises(Exception):
        redis.xadd("mystream", "*", {})


def test_xadd_with_limit(redis: Redis):
    """Test XADD with LIMIT option for trimming"""
    # Add many entries
    for i in range(100):
        redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Add with maxlen and limit
    redis.xadd("mystream", "*", {"field": "limited"}, maxlen=50, limit=10)

    # The stream should be trimmed but might not reach exactly 50 due to limit
    length = redis.xlen("mystream")
    assert length > 50  # Should be more than 50 due to limit constraint


def test_xadd_auto_sequence_number(redis: Redis):
    """Test XADD with auto-sequence number format <ms>-* (Redis 8+)"""
    # Use a specific timestamp with auto-sequence
    timestamp_ms = 1609459200000
    stream_id_pattern = f"{timestamp_ms}-*"
    
    result = redis.xadd("mystream", stream_id_pattern, {"field": "value"})
    
    # Verify the result has the correct timestamp
    assert isinstance(result, str)
    assert result.startswith(f"{timestamp_ms}-")
    
    # The sequence number should be auto-generated (0 for first entry)
    parts = result.split("-")
    assert len(parts) == 2
    assert parts[0] == str(timestamp_ms)
    assert parts[1].isdigit()  # Sequence number should be numeric


def test_xadd_auto_sequence_multiple_entries(redis: Redis):
    """Test multiple XADD calls with same timestamp but auto-sequence"""
    timestamp_ms = 1609459200000
    stream_id_pattern = f"{timestamp_ms}-*"
    
    # Add multiple entries with same timestamp
    id1 = redis.xadd("mystream", stream_id_pattern, {"field": "value1"})
    id2 = redis.xadd("mystream", stream_id_pattern, {"field": "value2"})
    id3 = redis.xadd("mystream", stream_id_pattern, {"field": "value3"})
    
    # All should have same timestamp but different sequence numbers
    assert id1.startswith(f"{timestamp_ms}-")
    assert id2.startswith(f"{timestamp_ms}-")
    assert id3.startswith(f"{timestamp_ms}-")
    
    # Sequence numbers should be incrementing
    seq1 = int(id1.split("-")[1])
    seq2 = int(id2.split("-")[1])
    seq3 = int(id3.split("-")[1])
    
    assert seq2 > seq1
    assert seq3 > seq2


def test_xadd_auto_sequence_with_options(redis: Redis):
    """Test XADD with auto-sequence and other options like maxlen"""
    timestamp_ms = 1609459200000
    
    # Add with auto-sequence and maxlen
    result = redis.xadd(
        "mystream",
        f"{timestamp_ms}-*",
        {"field": "value"},
        maxlen=10,
        approximate_trim=True
    )
    
    assert isinstance(result, str)
    assert result.startswith(f"{timestamp_ms}-")


def test_xadd_mixed_id_formats(redis: Redis):
    """Test XADD with different ID formats in same stream"""
    # Fully automatic
    id1 = redis.xadd("mystream", "*", {"type": "auto"})
    
    # Auto-sequence with specific timestamp
    id2 = redis.xadd("mystream", "1609459200000-*", {"type": "auto-seq"})
    
    # Explicit ID (must be greater than previous IDs)
    id3 = redis.xadd("mystream", "9999999999999-0", {"type": "explicit"})
    
    # All should be valid stream IDs
    assert "-" in id1
    assert "-" in id2
    assert "-" in id3
    
    # Verify all entries exist
    entries = redis.xrange("mystream")
    assert len(entries) == 3


def test_xadd_auto_sequence_format_validation(redis: Redis):
    """Test that auto-sequence format is properly handled"""
    # Valid format: <number>-*
    result = redis.xadd("mystream", "1234567890-*", {"field": "value"})
    assert result.startswith("1234567890-")
    
    # The asterisk tells Redis to auto-generate the sequence
    parts = result.split("-")
    assert len(parts) == 2
    assert parts[1].isdigit()
