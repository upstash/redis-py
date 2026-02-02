"""
Tests for XACKDEL command (async version).
"""

import pytest_asyncio
from pytest import mark

from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture
async def flush_db(async_redis: Redis):
    await async_redis.flushdb()
    yield


@mark.asyncio
async def test_xackdel_single_message(async_redis: Redis) -> None:
    """Test XACKDEL with a single message"""
    stream_key = "test_stream"
    group = "test_group"
    consumer = "test_consumer"

    id1 = await async_redis.xadd(stream_key, "*", {"field": "value1"})

    await async_redis.xgroup_create(stream_key, group, "0")
    await async_redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    result = await async_redis.xackdel(stream_key, group, id1)

    assert isinstance(result, list)
    assert len(result) == 1


@mark.asyncio
async def test_xackdel_multiple_messages(async_redis: Redis) -> None:
    """Test XACKDEL with multiple messages"""
    stream_key = "test_stream_multiple"
    group = "test_group_multiple"
    consumer = "test_consumer"

    id1 = await async_redis.xadd(stream_key, "*", {"field": "value1"})
    id2 = await async_redis.xadd(stream_key, "*", {"field": "value2"})

    await async_redis.xgroup_create(stream_key, group, "0")
    await async_redis.xreadgroup(group, consumer, {stream_key: ">"}, count=2)

    result = await async_redis.xackdel(stream_key, group, id1, id2)

    assert isinstance(result, list)
    assert len(result) == 2


@mark.asyncio
async def test_xackdel_with_keepref_option(async_redis: Redis) -> None:
    """Test XACKDEL with KEEPREF option"""
    stream_key = "test_stream_keepref"
    group = "test_group_keepref"
    consumer = "test_consumer"

    id1 = await async_redis.xadd(stream_key, "*", {"field": "value1"})

    await async_redis.xgroup_create(stream_key, group, "0")
    await async_redis.xreadgroup(group, consumer, {stream_key: ">"}, count=1)

    result = await async_redis.xackdel(stream_key, group, id1, option="KEEPREF")

    assert isinstance(result, list)
    assert len(result) == 1


@mark.asyncio
async def test_xackdel_with_acked_option(async_redis: Redis) -> None:
    """Test XACKDEL with ACKED option"""
    stream_key = "test_stream_acked"
    group = "test_group_acked"
    consumer = "test_consumer"

    id1 = await async_redis.xadd(stream_key, "*", {"field": "value1"})
    id2 = await async_redis.xadd(stream_key, "*", {"field": "value2"})

    await async_redis.xgroup_create(stream_key, group, "0")
    await async_redis.xreadgroup(group, consumer, {stream_key: ">"}, count=2)

    await async_redis.xack(stream_key, group, id1, id2)

    result = await async_redis.xackdel(stream_key, group, id1, id2, option="ACKED")

    assert isinstance(result, list)
    assert len(result) == 2
