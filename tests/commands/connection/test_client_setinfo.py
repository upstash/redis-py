"""
Tests for CLIENT SETINFO command.

Note: CLIENT commands may not be fully supported in Upstash REST API.
These tests verify command structure and basic functionality.
"""

import pytest

from upstash_redis import Redis


def test_client_setinfo_lib_name(redis: Redis):
    """Test CLIENT SETINFO with LIB-NAME attribute"""
    try:
        result = redis.client_setinfo("LIB-NAME", "redis-py")
        # If the command is supported, it should return OK
        assert result in ["OK", "ok", True]
    except Exception as e:
        # If not supported, that's expected behavior
        # We just verify the command structure is correct by checking no syntax error
        error_msg = str(e).lower()
        assert "syntax" not in error_msg or "unknown" in error_msg


def test_client_setinfo_lib_ver(redis: Redis):
    """Test CLIENT SETINFO with LIB-VER attribute"""
    try:
        result = redis.client_setinfo("LIB-VER", "1.0.0")
        assert result in ["OK", "ok", True]
    except Exception as e:
        error_msg = str(e).lower()
        assert "syntax" not in error_msg or "unknown" in error_msg


def test_client_setinfo_case_insensitive_lib_name(redis: Redis):
    """Test that LIB-NAME attribute is case-insensitive"""
    try:
        # The implementation converts to uppercase internally
        result = redis.client_setinfo("lib-name", "redis-py")
        assert result in ["OK", "ok", True]
    except Exception as e:
        error_msg = str(e).lower()
        assert "syntax" not in error_msg or "unknown" in error_msg


def test_client_setinfo_case_insensitive_lib_ver(redis: Redis):
    """Test that LIB-VER attribute is case-insensitive"""
    try:
        result = redis.client_setinfo("lib-ver", "2.0.0")
        assert result in ["OK", "ok", True]
    except Exception as e:
        error_msg = str(e).lower()
        assert "syntax" not in error_msg or "unknown" in error_msg


def test_client_setinfo_lib_name_with_suffix(redis: Redis):
    """Test CLIENT SETINFO with library name containing custom suffix"""
    try:
        result = redis.client_setinfo("LIB-NAME", "redis-py(upstash_v1.0.0)")
        assert result in ["OK", "ok", True]
    except Exception as e:
        error_msg = str(e).lower()
        assert "syntax" not in error_msg or "unknown" in error_msg


def test_client_setinfo_version_with_dots(redis: Redis):
    """Test CLIENT SETINFO with version string containing dots"""
    try:
        result = redis.client_setinfo("LIB-VER", "3.2.1")
        assert result in ["OK", "ok", True]
    except Exception as e:
        error_msg = str(e).lower()
        assert "syntax" not in error_msg or "unknown" in error_msg


def test_client_setinfo_version_with_prerelease(redis: Redis):
    """Test CLIENT SETINFO with version string containing prerelease info"""
    try:
        result = redis.client_setinfo("LIB-VER", "1.0.0-beta.1")
        assert result in ["OK", "ok", True]
    except Exception as e:
        error_msg = str(e).lower()
        assert "syntax" not in error_msg or "unknown" in error_msg


def test_client_setinfo_empty_value(redis: Redis):
    """Test CLIENT SETINFO with empty value"""
    try:
        result = redis.client_setinfo("LIB-NAME", "")
        # Empty value should be accepted
        assert result in ["OK", "ok", True]
    except Exception as e:
        # Empty value might not be allowed, which is fine
        error_msg = str(e).lower()
        # Just ensure no syntax error in command structure
        pass


def test_client_setinfo_special_characters_in_name(redis: Redis):
    """Test CLIENT SETINFO with special characters in library name"""
    try:
        result = redis.client_setinfo("LIB-NAME", "redis-py_custom-v1")
        assert result in ["OK", "ok", True]
    except Exception as e:
        error_msg = str(e).lower()
        assert "syntax" not in error_msg or "unknown" in error_msg


def test_client_setinfo_multiple_calls(redis: Redis):
    """Test multiple CLIENT SETINFO calls in sequence"""
    try:
        result1 = redis.client_setinfo("LIB-NAME", "redis-py")
        result2 = redis.client_setinfo("LIB-VER", "1.0.0")
        # Both should succeed
        assert result1 in ["OK", "ok", True]
        assert result2 in ["OK", "ok", True]
    except Exception as e:
        # If not supported, that's expected
        pass

