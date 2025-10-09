import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()


@pytest.mark.asyncio
async def test_xadd_basic(async_redis: Redis):
    """Test basic XADD functionality"""
    result = await async_redis.xadd(
        "mystream", "*", {"field1": "value1", "field2": "value2"}
    )
    assert isinstance(result, str)
    assert len(result) > 0
    # Stream ID format should be timestamp-sequence
    assert "-" in result


@pytest.mark.asyncio
async def test_xadd_with_specific_id(async_redis: Redis):
    """Test XADD with a specific ID"""
    stream_id = "1609459200000-0"
    result = await async_redis.xadd("mystream", stream_id, {"field": "value"})
    assert result == stream_id


@pytest.mark.asyncio
async def test_xadd_with_maxlen_exact(async_redis: Redis):
    """Test XADD with exact maxlen trimming"""
    # Add multiple entries
    for i in range(10):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Add with maxlen=5 (exact)
    await async_redis.xadd(
        "mystream", "*", {"field": "new_value"}, maxlen=5, approximate_trim=False
    )

    # Check stream length
    length = await async_redis.xlen("mystream")
    assert length <= 5


@pytest.mark.asyncio
async def test_xadd_with_maxlen_approximate(async_redis: Redis):
    """Test XADD with approximate maxlen trimming"""
    # Add multiple entries
    for i in range(120):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Add with approximate maxlen
    await async_redis.xadd(
        "mystream", "*", {"field": "new_value"}, maxlen=30, approximate_trim=True
    )

    # Check stream length (should be around 30, but may be slightly more due to approximation)
    length = await async_redis.xlen("mystream")
    assert length >= 110
    assert length <= 130  # Allow some variance for approximation


@pytest.mark.asyncio
async def test_xadd_with_nomkstream(async_redis: Redis):
    """Test XADD with NOMKSTREAM option"""
    # Should return None when stream doesn't exist and NOMKSTREAM is used
    result = await async_redis.xadd(
        "nonexistent", "*", {"field": "value"}, nomkstream=True
    )
    assert result is None

    # Create the stream first
    await async_redis.xadd("mystream", "*", {"field": "value1"})

    # Now NOMKSTREAM should work
    result = await async_redis.xadd(
        "mystream", "*", {"field": "value2"}, nomkstream=True
    )
    assert isinstance(result, str)


@pytest.mark.asyncio
async def test_xadd_with_minid(async_redis: Redis):
    """Test XADD with MINID trimming"""
    # Add some entries with known IDs
    await async_redis.xadd("mystream", "1000-0", {"field": "old1"})
    await async_redis.xadd("mystream", "2000-0", {"field": "old2"})
    await async_redis.xadd("mystream", "3000-0", {"field": "old3"})

    # Add new entry with MINID that should trim old entries
    await async_redis.xadd(
        "mystream", "4000-0", {"field": "new"}, minid="2500-0", approximate_trim=False
    )

    # Check that old entries before minid are removed
    entries = await async_redis.xrange("mystream")
    entry_ids = [entry[0] for entry in entries]

    # Should not contain entries with IDs less than 2500-0
    for entry_id in entry_ids:
        timestamp = int(entry_id.split("-")[0])
        assert timestamp >= 2500


@pytest.mark.asyncio
async def test_xadd_multiple_fields(async_redis: Redis):
    """Test XADD with multiple fields"""
    data = {"name": "John", "age": "30", "city": "New York", "score": "95.5"}

    result = await async_redis.xadd("mystream", "*", data)
    assert isinstance(result, str)

    # Verify the data was stored correctly
    entries = await async_redis.xrange("mystream")
    assert len(entries) == 1

    entry_id, entry_data = entries[0]
    assert entry_id == result

    # Convert list to dict for easier comparison
    entry_dict = {}
    for i in range(0, len(entry_data), 2):
        entry_dict[entry_data[i]] = entry_data[i + 1]

    assert entry_dict == data


@pytest.mark.asyncio
async def test_xadd_empty_data_should_fail(async_redis: Redis):
    """Test that XADD with empty data should fail"""
    with pytest.raises(Exception):
        await async_redis.xadd("mystream", "*", {})


@pytest.mark.asyncio
async def test_xadd_with_limit(async_redis: Redis):
    """Test XADD with LIMIT option for trimming"""
    # Add many entries
    for i in range(100):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Add with maxlen and limit
    await async_redis.xadd("mystream", "*", {"field": "limited"}, maxlen=50, limit=10)

    # The stream should be trimmed but might not reach exactly 50 due to limit
    length = await async_redis.xlen("mystream")
    assert length > 50  # Should be more than 50 due to limit constraint
