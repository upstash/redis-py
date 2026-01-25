"""
Tests for HSETEX command (async version).
"""

import pytest_asyncio
from pytest import mark

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()
    yield


@mark.asyncio
async def test_hsetex_single_field_with_ex(async_redis: Redis) -> None:
    """Test HSETEX with a single field and EX expiration"""
    result = await async_redis.hsetex("myhash", field="field1", value="value1", ex=10)

    assert result >= 0
    value = await async_redis.hget("myhash", "field1")
    assert value == "value1"
    # Note: HSETEX may not set TTL on the hash itself in all Redis versions
    # Just verify the command executes successfully


@mark.asyncio
async def test_hsetex_multiple_fields_with_ex(async_redis: Redis) -> None:
    """Test HSETEX with multiple fields and EX expiration"""
    result = await async_redis.hsetex(
        "myhash",
        values={"field1": "value1", "field2": "value2", "field3": "value3"},
        ex=10,
    )

    assert result >= 0
    assert await async_redis.hget("myhash", "field1") == "value1"
    assert await async_redis.hget("myhash", "field2") == "value2"
    assert await async_redis.hget("myhash", "field3") == "value3"


@mark.asyncio
async def test_hsetex_with_fnx(async_redis: Redis) -> None:
    """Test HSETEX with FNX (field not exists) option"""
    result1 = await async_redis.hsetex(
        "myhash", field="field1", value="value1", fnx=True
    )
    assert result1 >= 0

    result2 = await async_redis.hsetex(
        "myhash", field="field1", value="value2", fnx=True
    )
    assert result2 == 0

    value = await async_redis.hget("myhash", "field1")
    assert value == "value1"


@mark.asyncio
async def test_hsetex_with_keepttl(async_redis: Redis) -> None:
    """Test HSETEX with KEEPTTL option"""
    await async_redis.hsetex("myhash", field="field1", value="value1", ex=100)

    await async_redis.hsetex("myhash", field="field2", value="value2", keepttl=True)

    # Verify both fields exist
    assert await async_redis.hexists("myhash", "field1") == 1
    assert await async_redis.hexists("myhash", "field2") == 1
