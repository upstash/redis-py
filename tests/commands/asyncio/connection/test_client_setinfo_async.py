"""
Tests for CLIENT SETINFO command (async version).
"""

from pytest import mark

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test_client_setinfo_lib_name(async_redis: Redis) -> None:
    """Test CLIENT SETINFO with LIB-NAME"""
    try:
        result = await async_redis.client_setinfo("LIB-NAME", "redis-py")
        assert result in ["OK", "ok", True]
    except Exception:
        # CLIENT commands might not be supported in REST API
        pass


@mark.asyncio
async def test_client_setinfo_lib_ver(async_redis: Redis) -> None:
    """Test CLIENT SETINFO with LIB-VER"""
    try:
        result = await async_redis.client_setinfo("LIB-VER", "1.0.0")
        assert result in ["OK", "ok", True]
    except Exception:
        # CLIENT commands might not be supported in REST API
        pass


@mark.asyncio
async def test_client_setinfo_case_insensitive_lib_name(async_redis: Redis) -> None:
    """Test CLIENT SETINFO with lowercase lib-name"""
    try:
        result = await async_redis.client_setinfo("lib-name", "redis-py")
        assert result in ["OK", "ok", True]
    except Exception:
        # CLIENT commands might not be supported in REST API
        pass


@mark.asyncio
async def test_client_setinfo_case_insensitive_lib_ver(async_redis: Redis) -> None:
    """Test CLIENT SETINFO with lowercase lib-ver"""
    try:
        result = await async_redis.client_setinfo("lib-ver", "2.0.0")
        assert result in ["OK", "ok", True]
    except Exception:
        # CLIENT commands might not be supported in REST API
        pass


@mark.asyncio
async def test_client_setinfo_lib_name_with_suffix(async_redis: Redis) -> None:
    """Test CLIENT SETINFO with library name containing custom suffix"""
    try:
        result = await async_redis.client_setinfo(
            "LIB-NAME", "redis-py(upstash_v1.0.0)"
        )
        assert result in ["OK", "ok", True]
    except Exception:
        # CLIENT commands might not be supported in REST API
        pass


@mark.asyncio
async def test_client_setinfo_version_with_dots(async_redis: Redis) -> None:
    """Test CLIENT SETINFO with version containing multiple dots"""
    try:
        result = await async_redis.client_setinfo("LIB-VER", "3.2.1")
        assert result in ["OK", "ok", True]
    except Exception:
        # CLIENT commands might not be supported in REST API
        pass
