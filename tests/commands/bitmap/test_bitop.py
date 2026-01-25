"""
Tests for BITOP command with extended operations.
"""

import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_and_setup(redis: Redis):
    """Setup test keys for bitop operations"""
    redis.flushdb()
    
    # Setup test keys with known bit patterns
    # key1: bits 0,1,2 set (binary: 11100000 = 0xE0 = 224)
    redis.setbit("key1", 0, 1)
    redis.setbit("key1", 1, 1)
    redis.setbit("key1", 2, 1)
    
    # key2: bits 1,2,3 set (binary: 01110000 = 0x70 = 112)
    redis.setbit("key2", 1, 1)
    redis.setbit("key2", 2, 1)
    redis.setbit("key2", 3, 1)
    
    # key3: bits 2,3,4 set (binary: 00111000 = 0x38 = 56)
    redis.setbit("key3", 2, 1)
    redis.setbit("key3", 3, 1)
    redis.setbit("key3", 4, 1)
    
    yield
    
    redis.flushdb()


def test_bitop_and(redis: Redis):
    """Test BITOP AND operation"""
    result = redis.bitop("AND", "dest", "key1", "key2")
    assert result == 1  # Length of result in bytes
    
    # Bits 1 and 2 should be set (both keys have them)
    assert redis.getbit("dest", 0) == 0
    assert redis.getbit("dest", 1) == 1
    assert redis.getbit("dest", 2) == 1
    assert redis.getbit("dest", 3) == 0


def test_bitop_or(redis: Redis):
    """Test BITOP OR operation"""
    result = redis.bitop("OR", "dest", "key1", "key2")
    assert result == 1
    
    # Bits 0,1,2,3 should be set (union of both keys)
    assert redis.getbit("dest", 0) == 1
    assert redis.getbit("dest", 1) == 1
    assert redis.getbit("dest", 2) == 1
    assert redis.getbit("dest", 3) == 1
    assert redis.getbit("dest", 4) == 0


def test_bitop_xor(redis: Redis):
    """Test BITOP XOR operation"""
    result = redis.bitop("XOR", "dest", "key1", "key2")
    assert result == 1
    
    # Only bits that differ should be set
    assert redis.getbit("dest", 0) == 1  # Only in key1
    assert redis.getbit("dest", 1) == 0  # In both (XOR = 0)
    assert redis.getbit("dest", 2) == 0  # In both (XOR = 0)
    assert redis.getbit("dest", 3) == 1  # Only in key2


def test_bitop_not(redis: Redis):
    """Test BITOP NOT operation"""
    result = redis.bitop("NOT", "dest", "key1")
    assert result == 1
    
    # All bits should be inverted
    assert redis.getbit("dest", 0) == 0
    assert redis.getbit("dest", 1) == 0
    assert redis.getbit("dest", 2) == 0
    assert redis.getbit("dest", 3) == 1


def test_bitop_diff(redis: Redis):
    """Test BITOP DIFF operation - sets bits that are in X but not in any Y"""
    result = redis.bitop("DIFF", "dest", "key1", "key2", "key3")
    assert result == 1  # Returns length of result in bytes
    
    # DIFF: bits that are in X but not in any Y
    # key1: bits 0,1,2 | key2: bits 1,2,3 | key3: bits 2,3,4
    # Only bit 0 is in key1 but not in key2 or key3
    assert redis.getbit("dest", 0) == 1  # In key1, not in others
    assert redis.getbit("dest", 1) == 0  # In key1 and key2
    assert redis.getbit("dest", 2) == 0  # In all keys
    assert redis.getbit("dest", 3) == 0  # Not in key1


def test_bitop_diff1(redis: Redis):
    """Test BITOP DIFF1 operation - sets bits that are in Y but not in X"""
    result = redis.bitop("DIFF1", "dest", "key1", "key2", "key3")
    assert result == 1  # Returns length of result in bytes
    
    # DIFF1: bits that are in Y (key2, key3) but not in X (key1)
    # key1: bits 0,1,2 | key2: bits 1,2,3 | key3: bits 2,3,4
    # Bits 3 and 4 are in Y keys but not in key1
    assert redis.getbit("dest", 0) == 0  # In key1
    assert redis.getbit("dest", 1) == 0  # In key1
    assert redis.getbit("dest", 2) == 0  # In key1
    assert redis.getbit("dest", 3) == 1  # In Y (key2, key3) but not key1
    assert redis.getbit("dest", 4) == 1  # In Y (key3) but not key1


def test_bitop_andor(redis: Redis):
    """Test BITOP ANDOR operation - bit set if in X and in one or more Y"""
    # key1 is X, key2 and key3 are Y
    result = redis.bitop("ANDOR", "dest", "key1", "key2", "key3")
    assert result == 1
    
    # Bit must be in key1 AND (key2 OR key3)
    # key1: bits 0,1,2 | key2: bits 1,2,3 | key3: bits 2,3,4
    assert redis.getbit("dest", 0) == 0  # In key1 but not in key2 or key3
    assert redis.getbit("dest", 1) == 1  # In key1 and key2
    assert redis.getbit("dest", 2) == 1  # In key1 and both key2, key3
    assert redis.getbit("dest", 3) == 0  # Not in key1


def test_bitop_one(redis: Redis):
    """Test BITOP ONE operation - bit set if in exactly one source"""
    result = redis.bitop("ONE", "dest", "key1", "key2", "key3")
    assert result == 1
    
    # Count bits across all keys - must be in exactly one
    # key1: bits 0,1,2 | key2: bits 1,2,3 | key3: bits 2,3,4
    assert redis.getbit("dest", 0) == 1  # Only in key1
    assert redis.getbit("dest", 1) == 0  # In key1 and key2 (not exactly one)
    assert redis.getbit("dest", 2) == 0  # In all three (not exactly one)
    assert redis.getbit("dest", 3) == 0  # In key2 and key3 (not exactly one)
    assert redis.getbit("dest", 4) == 1  # Only in key3


def test_bitop_without_source_keys(redis: Redis):
    """Test BITOP requires at least one source key"""
    with pytest.raises(Exception, match="At least one source key must be specified"):
        redis.bitop("AND", "dest")


def test_bitop_not_with_multiple_keys(redis: Redis):
    """Test BITOP NOT only accepts one source key"""
    with pytest.raises(Exception, match='The "NOT" operation takes only one source key'):
        redis.bitop("NOT", "dest", "key1", "key2")


def test_bitop_case_insensitive(redis: Redis):
    """Test BITOP operation names are case-insensitive"""
    # Lowercase should work
    result = redis.bitop("and", "dest", "key1", "key2")
    assert result == 1


def test_bitop_multiple_sources(redis: Redis):
    """Test BITOP with many source keys"""
    # Create more keys
    redis.setbit("key4", 5, 1)
    redis.setbit("key5", 6, 1)
    
    result = redis.bitop("OR", "dest", "key1", "key2", "key3", "key4", "key5")
    assert result == 1
    
    # Should have bits from all sources
    assert redis.getbit("dest", 0) == 1
    assert redis.getbit("dest", 5) == 1
    assert redis.getbit("dest", 6) == 1


def test_bitop_nonexistent_keys(redis: Redis):
    """Test BITOP with non-existent keys treats them as zero"""
    result = redis.bitop("OR", "dest", "key1", "nonexistent")
    assert result == 1
    
    # Should equal key1 since nonexistent is all zeros
    assert redis.getbit("dest", 0) == 1
    assert redis.getbit("dest", 1) == 1
    assert redis.getbit("dest", 2) == 1


def test_bitop_overwrite_destination(redis: Redis):
    """Test BITOP overwrites existing destination key"""
    # Set destination with some data
    redis.set("dest", "old_data")
    
    # BITOP should overwrite it
    result = redis.bitop("AND", "dest", "key1", "key2")
    assert result == 1
    
    # Destination should now have the AND result, not old_data
    assert redis.get("dest") != "old_data"


def test_bitop_empty_result(redis: Redis):
    """Test BITOP when result would be all zeros"""
    redis.setbit("empty1", 10, 0)
    redis.setbit("empty2", 10, 0)
    
    result = redis.bitop("AND", "dest", "empty1", "empty2")
    assert result >= 1  # Should return the length


def test_bitop_diff_requires_multiple_keys(redis: Redis):
    """Test BITOP DIFF requires at least two source keys"""
    with pytest.raises(Exception, match="BITOP DIFF must be called with at least two source keys"):
        redis.bitop("DIFF", "dest", "key1")


def test_bitop_diff1_requires_multiple_keys(redis: Redis):
    """Test BITOP DIFF1 requires at least two source keys"""
    with pytest.raises(Exception, match="BITOP DIFF1 must be called with at least two source keys"):
        redis.bitop("DIFF1", "dest", "key1")


def test_bitop_andor_requires_multiple_keys(redis: Redis):
    """Test BITOP ANDOR requires at least two source keys"""
    with pytest.raises(Exception, match="BITOP ANDOR must be called with at least two source keys"):
        redis.bitop("ANDOR", "dest", "key1")

