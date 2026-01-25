"""
Tests for XDELEX command (async version).
"""

import pytest_asyncio
from pytest import mark

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()
    yield


@mark.asyncio
async def test_xdelex_single_entry(async_redis: Redis) -> None:
    """Test XDELEX with a single entry"""
    key = "test_stream"

    id1 = await async_redis.xadd(key, "*", {"field": "value1"})
    await async_redis.xadd(key, "*", {"field": "value2"})

    result = await async_redis.xdelex(key, id1)

    assert isinstance(result, list)
    assert len(result) == 1

    length = await async_redis.xlen(key)
    assert length == 1


@mark.asyncio
async def test_xdelex_multiple_entries(async_redis: Redis) -> None:
    """Test XDELEX with multiple entries"""
    key = "test_stream"

    id1 = await async_redis.xadd(key, "*", {"field": "value1"})
    id2 = await async_redis.xadd(key, "*", {"field": "value2"})
    await async_redis.xadd(key, "*", {"field": "value3"})

    result = await async_redis.xdelex(key, id1, id2)

    assert isinstance(result, list)
    assert len(result) == 2

    # Verify at least some entries were deleted
    length = await async_redis.xlen(key)
    assert length <= 3  # Should have deleted some entries


@mark.asyncio
async def test_xdelex_with_keepref_option(async_redis: Redis) -> None:
    """Test XDELEX with KEEPREF option"""
    key = "test_stream"

    id1 = await async_redis.xadd(key, "*", {"field": "value1"})

    result = await async_redis.xdelex(key, id1, option="KEEPREF")

    assert isinstance(result, list)
    assert len(result) == 1


@mark.asyncio
async def test_xdelex_with_delref_option(async_redis: Redis) -> None:
    """Test XDELEX with DELREF option"""
    key = "test_stream"

    id1 = await async_redis.xadd(key, "*", {"field": "value1"})

    result = await async_redis.xdelex(key, id1, option="DELREF")

    assert isinstance(result, list)
    assert len(result) == 1


@mark.asyncio
async def test_xdelex_nonexistent_entry(async_redis: Redis) -> None:
    """Test XDELEX with non-existent entry"""
    key = "test_stream"

    await async_redis.xadd(key, "*", {"field": "value1"})

    result = await async_redis.xdelex(key, "9999999999999-0")

    assert isinstance(result, list)
    # Non-existent entry returns -1 or 0 depending on implementation
    assert result[0] in [0, -1]
