"""
Tests for HGETDEL command (async version).
"""

import pytest_asyncio
from pytest import mark

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()


@mark.asyncio
async def test_hgetdel_single_field(async_redis: Redis) -> None:
    """Test HGETDEL with a single field"""
    await async_redis.hset("myhash", "field1", "value1")
    await async_redis.hset("myhash", "field2", "value2")

    result = await async_redis.hgetdel("myhash", "field1")

    # Returns list of values (Redis raw format)
    assert result == ["value1"]
    assert await async_redis.hexists("myhash", "field1") == 0
    assert await async_redis.hexists("myhash", "field2") == 1


@mark.asyncio
async def test_hgetdel_multiple_fields(async_redis: Redis) -> None:
    """Test HGETDEL with multiple fields"""
    await async_redis.hset("myhash", "field1", "value1")
    await async_redis.hset("myhash", "field2", "value2")
    await async_redis.hset("myhash", "field3", "value3")

    result = await async_redis.hgetdel("myhash", "field1", "field2")

    # Returns list of values (Redis raw format)
    assert result == ["value1", "value2"]
    assert await async_redis.hexists("myhash", "field1") == 0
    assert await async_redis.hexists("myhash", "field2") == 0
    assert await async_redis.hexists("myhash", "field3") == 1


@mark.asyncio
async def test_hgetdel_non_existent_field(async_redis: Redis) -> None:
    """Test HGETDEL with a non-existent field"""
    await async_redis.hset("myhash", "field1", "value1")

    result = await async_redis.hgetdel("myhash", "nonexistent")

    # Returns list with None for non-existent fields
    assert result == [None]
    assert await async_redis.hexists("myhash", "field1") == 1


@mark.asyncio
async def test_hgetdel_deletes_hash_when_empty(async_redis: Redis) -> None:
    """Test that HGETDEL deletes the hash when the last field is removed"""
    await async_redis.hset("myhash", "field1", "value1")

    result = await async_redis.hgetdel("myhash", "field1")

    # Returns list of values (Redis raw format)
    assert result == ["value1"]
    assert await async_redis.exists("myhash") == 0
