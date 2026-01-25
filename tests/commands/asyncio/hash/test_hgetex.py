"""
Tests for HGETEX command (async version).
"""

import pytest_asyncio
from pytest import mark

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()


@mark.asyncio
async def test_hgetex_basic(async_redis: Redis) -> None:
    """Test basic HGETEX functionality"""
    await async_redis.hset("myhash", "field1", "value1")
    await async_redis.hset("myhash", "field2", "value2")

    result = await async_redis.hgetex("myhash", "field1", "field2")

    # Returns list of values (Redis raw format)
    assert result == ["value1", "value2"]


@mark.asyncio
async def test_hgetex_with_ex(async_redis: Redis) -> None:
    """Test HGETEX with EX (seconds) expiration"""
    await async_redis.hset("myhash", "field1", "value1")

    result = await async_redis.hgetex("myhash", "field1", ex=10)

    # Returns list of values (Redis raw format)
    assert result == ["value1"]
    # Note: HGETEX sets per-field expiration, not hash expiration
    # The command executes successfully


@mark.asyncio
async def test_hgetex_with_px(async_redis: Redis) -> None:
    """Test HGETEX with PX (milliseconds) expiration"""
    await async_redis.hset("myhash", "field1", "value1")

    result = await async_redis.hgetex("myhash", "field1", px=10000)

    # Returns list of values (Redis raw format)
    assert result == ["value1"]
    # Note: HGETEX sets per-field expiration, not hash expiration
    # The command executes successfully


@mark.asyncio
async def test_hgetex_multiple_fields(async_redis: Redis) -> None:
    """Test HGETEX with multiple fields"""
    await async_redis.hset("myhash", "field1", "value1")
    await async_redis.hset("myhash", "field2", "value2")
    await async_redis.hset("myhash", "field3", "value3")

    result = await async_redis.hgetex("myhash", "field1", "field2", "field3")

    # Returns list of values (Redis raw format)
    assert result == ["value1", "value2", "value3"]
