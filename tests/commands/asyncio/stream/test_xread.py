import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()


@pytest.mark.asyncio
async def test_xread_basic(async_redis: Redis):
    """Test basic XREAD functionality"""
    # Add some entries
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    await async_redis.xadd("mystream", "*", {"field": "value2"})

    # Read from beginning
    result = await async_redis.xread({"mystream": "0-0"})

    assert len(result) == 1
    stream_name, entries = result[0]
    assert stream_name == "mystream"
    assert len(entries) >= 2


@pytest.mark.asyncio
async def test_xread_with_count(async_redis: Redis):
    """Test XREAD with COUNT option"""
    # Add multiple entries
    for i in range(5):
        await async_redis.xadd("mystream", "*", {"field": f"value{i}"})

    # Read with count limit
    result = await async_redis.xread({"mystream": "0-0"}, count=2)
    stream_name, entries = result[0]
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_xread_multiple_streams(async_redis: Redis):
    """Test XREAD with multiple streams"""
    # Add entries to multiple streams
    await async_redis.xadd("stream1", "*", {"field": "value1"})
    await async_redis.xadd("stream2", "*", {"field": "value2"})

    # Read from both streams
    result = await async_redis.xread({"stream1": "0-0", "stream2": "0-0"})

    assert len(result) == 2
    stream_names = [stream[0] for stream in result]
    assert "stream1" in stream_names
    assert "stream2" in stream_names


@pytest.mark.asyncio
async def test_xread_from_specific_id(async_redis: Redis):
    """Test XREAD from a specific stream ID"""
    # Add entries
    await async_redis.xadd("mystream", "*", {"field": "value1"})
    id2 = await async_redis.xadd("mystream", "*", {"field": "value2"})
    id3 = await async_redis.xadd("mystream", "*", {"field": "value3"})

    # Read from id2 onwards (should get id3)
    result = await async_redis.xread({"mystream": id2})

    stream_name, entries = result[0]
    assert len(entries) == 1
    assert entries[0][0] == id3


@pytest.mark.asyncio
async def test_xread_nonexistent_stream(async_redis: Redis):
    """Test XREAD on nonexistent stream"""
    result = await async_redis.xread({"nonexistent": "0-0"})
    assert result == []


@pytest.mark.asyncio
async def test_xread_from_end(async_redis: Redis):
    """Test XREAD from end of stream (no new messages)"""
    # Add an entry
    await async_redis.xadd("mystream", "*", {"field": "value1"})

    # Read from $ (end of stream) - should return empty
    result = await async_redis.xread({"mystream": "$"})
    assert result == []
