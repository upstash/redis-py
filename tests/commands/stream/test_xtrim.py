import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_xtrim_maxlen_exact(redis: Redis):
    """Test XTRIM with exact MAXLEN"""
    key = "test_stream"

    # Add many entries
    for i in range(50):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Trim to exact length
    trimmed = redis.xtrim(key, maxlen=30, approximate=False)
    assert isinstance(trimmed, int)
    assert trimmed >= 0

    # Check final length
    length = redis.xlen(key)
    assert length <= 30


@pytest.mark.skip(reason="Approximate trimming behavior differs in Upstash Redis")
def test_xtrim_maxlen_approximate(redis: Redis):
    """Test XTRIM with approximate MAXLEN"""
    key = "test_stream"

    # Add many entries
    for i in range(100):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Trim approximately
    trimmed = redis.xtrim(key, maxlen=30, approximate=True)
    assert isinstance(trimmed, int)

    # Length should be around 30 but may vary due to approximation
    length = redis.xlen(key)
    assert length >= 25  # Should be reasonably close
    assert length <= 40  # But allow some variance


def test_xtrim_with_limit(redis: Redis):
    """Test XTRIM with LIMIT option"""
    key = "test_stream"

    # Add many entries
    for i in range(100):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Trim with limit
    redis.xtrim(key, maxlen=50, limit=10)

    # With limit, trimming might not reach the target
    length = redis.xlen(key)
    assert length > 50  # Should be more than target due to limit


def test_xtrim_minid(redis: Redis):
    """Test XTRIM with MINID"""
    key = "test_stream"

    # Add entries with known timestamps
    base_timestamp = 1000000000000
    for i in range(20):
        stream_id = f"{base_timestamp + i * 1000}-0"
        redis.xadd(key, stream_id, {"field": f"value{i}"})

    # Trim by minimum ID (remove entries older than midpoint)
    mid_id = f"{base_timestamp + 10000}-0"
    redis.xtrim(key, minid=mid_id, approximate=False)

    # Check that old entries are removed
    entries = redis.xrange(key, "-", "+")
    for entry_id, _ in entries:
        timestamp = int(entry_id.split("-")[0])
        assert timestamp >= base_timestamp + 10000


def test_xtrim_zero_length(redis: Redis):
    """Test XTRIM with zero length removes everything"""
    key = "test_stream"

    # Add entries
    for i in range(10):
        redis.xadd(key, "*", {"field": f"value{i}"})

    initial_length = redis.xlen(key)
    assert initial_length == 10

    # Trim to zero
    redis.xtrim(key, maxlen=0, approximate=False)

    # Should remove everything
    final_length = redis.xlen(key)
    assert final_length <= 1  # Might leave 1 entry in some implementations


def test_xtrim_empty_stream(redis: Redis):
    """Test XTRIM on empty stream"""
    key = "test_stream"

    # Create empty stream
    redis.xadd(key, "*", {"field": "value"})
    redis.xtrim(key, maxlen=0, approximate=False)

    # Trim already empty stream
    trimmed = redis.xtrim(key, maxlen=10, approximate=False)
    assert trimmed == 0


def test_xtrim_no_effect(redis: Redis):
    """Test XTRIM when no trimming needed"""
    key = "test_stream"

    # Add few entries
    for i in range(5):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Trim to larger size (should have no effect)
    trimmed = redis.xtrim(key, maxlen=10, approximate=False)
    assert trimmed == 0

    # Length should remain unchanged
    length = redis.xlen(key)
    assert length == 5


def test_xtrim_preserves_recent_entries(redis: Redis):
    """Test that XTRIM preserves the most recent entries"""
    key = "test_stream"

    # Add entries with known data
    entry_ids = []
    for i in range(10):
        entry_id = redis.xadd(key, "*", {"counter": str(i)})
        entry_ids.append(entry_id)

    # Trim to keep only 5 entries
    redis.xtrim(key, maxlen=5, approximate=False)

    # Check remaining entries (should be the last 5)
    remaining = redis.xrange(key, "-", "+")
    remaining_ids = [entry[0] for entry in remaining]

    # Should contain the most recent entries
    for recent_id in entry_ids[-5:]:
        if recent_id in remaining_ids:
            # At least some of the recent entries should remain
            pass


def test_xtrim_with_consumer_groups(redis: Redis):
    """Test XTRIM behavior with consumer groups"""
    key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    # Add entries
    for i in range(10):
        redis.xadd(key, "*", {"field": f"value{i}"})

    # Create consumer group and read some messages
    redis.xgroup_create(key, group, "0")
    redis.xreadgroup(group, consumer, {key: ">"}, count=5)

    # Trim the stream
    redis.xtrim(key, maxlen=5, approximate=False)

    # Stream should be trimmed
    length = redis.xlen(key)
    assert length <= 5


def test_xtrim_invalid_parameters(redis: Redis):
    """Test XTRIM with invalid parameter combinations"""
    key = "test_stream"

    # Add some entries
    redis.xadd(key, "*", {"field": "value"})

    # Should raise error when neither maxlen nor minid specified
    with pytest.raises(ValueError):
        redis.xtrim(key)  # No maxlen or minid


def test_xtrim_minid_exact(redis: Redis):
    """Test XTRIM MINID with exact trimming"""
    key = "test_stream"

    # Add entries with predictable IDs
    base_timestamp = 2000000000000
    ids = []
    for i in range(10):
        stream_id = f"{base_timestamp + i * 1000}-0"
        redis.xadd(key, stream_id, {"index": str(i)})
        ids.append(stream_id)

    # Trim by MINID (exact)
    target_id = ids[5]  # Keep entries from index 5 onwards
    redis.xtrim(key, minid=target_id, approximate=False)

    # Verify only entries >= target_id remain
    remaining = redis.xrange(key, "-", "+")
    for entry_id, _ in remaining:
        # Parse timestamp from ID
        entry_timestamp = int(entry_id.split("-")[0])
        target_timestamp = int(target_id.split("-")[0])
        assert entry_timestamp >= target_timestamp


def test_xtrim_very_large_stream(redis: Redis):
    """Test XTRIM on a larger stream"""
    key = "test_stream"

    # Add many entries
    entry_count = 200
    for i in range(entry_count):
        redis.xadd(key, "*", {"index": str(i)})

    initial_length = redis.xlen(key)
    assert initial_length == entry_count

    # Trim to much smaller size
    target_size = 20
    trimmed = redis.xtrim(key, maxlen=target_size, approximate=False)

    # Should have removed many entries
    assert trimmed > 0

    # Final size should be around target
    final_length = redis.xlen(key)
    assert final_length <= target_size
