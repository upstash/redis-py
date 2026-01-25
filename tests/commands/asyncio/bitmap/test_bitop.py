import pytest_asyncio
from pytest import mark, raises

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis


@pytest_asyncio.fixture(autouse=True)
async def setup_test_data(async_redis: Redis):
    """Setup test data for bitop operations"""
    await async_redis.flushdb()
    # Setup source strings for original tests
    await async_redis.set("string_as_bitop_source_1", "1234")
    await async_redis.set("string_as_bitop_source_2", "ABCD")
    yield
    await async_redis.flushdb()


@mark.asyncio
async def test_not_not_operation(async_redis: Redis) -> None:
    assert (
        await async_redis.bitop(
            "AND",
            "bitop_destination_1",
            "string_as_bitop_source_1",
            "string_as_bitop_source_2",
        )
        == 4
    )

    # Verify the result exists
    result = await execute_on_http("GET", "bitop_destination_1")
    assert result is not None
    assert len(result) == 4


@mark.asyncio
async def test_without_source_keys(async_redis: Redis) -> None:
    with raises(Exception) as exception:
        await async_redis.bitop("AND", "bitop_destination_1")

    assert str(exception.value) == "At least one source key must be specified."


@mark.asyncio
async def test_not_with_more_than_one_source_key(async_redis: Redis) -> None:
    with raises(Exception) as exception:
        await async_redis.bitop(
            "NOT",
            "bitop_destination_4",
            "string_as_bitop_source_1",
            "string_as_bitop_source_2",
        )

    assert (
        str(exception.value)
        == 'The "NOT" operation takes only one source key as argument.'
    )


@mark.asyncio
async def test_not(async_redis: Redis) -> None:
    assert (
        await async_redis.bitop(
            "NOT", "bitop_destination_4", "string_as_bitop_source_1"
        )
        == 4
    )


@mark.asyncio
async def test_diff(async_redis: Redis) -> None:
    """Test BITOP DIFF operation - sets bits that are in X but not in any Y"""
    # Setup test keys with known bit patterns
    await async_redis.setbit("diff_key1", 0, 1)
    await async_redis.setbit("diff_key1", 1, 1)
    await async_redis.setbit("diff_key1", 2, 1)

    await async_redis.setbit("diff_key2", 1, 1)
    await async_redis.setbit("diff_key2", 2, 1)
    await async_redis.setbit("diff_key2", 3, 1)

    await async_redis.setbit("diff_key3", 2, 1)
    await async_redis.setbit("diff_key3", 3, 1)
    await async_redis.setbit("diff_key3", 4, 1)

    result = await async_redis.bitop(
        "DIFF", "diff_dest", "diff_key1", "diff_key2", "diff_key3"
    )
    assert result == 1

    # Only bit 0 is in key1 but not in key2 or key3
    assert await async_redis.getbit("diff_dest", 0) == 1
    assert await async_redis.getbit("diff_dest", 1) == 0
    assert await async_redis.getbit("diff_dest", 2) == 0


@mark.asyncio
async def test_diff1(async_redis: Redis) -> None:
    """Test BITOP DIFF1 operation - sets bits that are in Y but not in X"""
    # Setup test keys with known bit patterns
    await async_redis.setbit("diff1_key1", 0, 1)
    await async_redis.setbit("diff1_key1", 1, 1)
    await async_redis.setbit("diff1_key1", 2, 1)

    await async_redis.setbit("diff1_key2", 1, 1)
    await async_redis.setbit("diff1_key2", 2, 1)
    await async_redis.setbit("diff1_key2", 3, 1)

    await async_redis.setbit("diff1_key3", 2, 1)
    await async_redis.setbit("diff1_key3", 3, 1)
    await async_redis.setbit("diff1_key3", 4, 1)

    result = await async_redis.bitop(
        "DIFF1", "diff1_dest", "diff1_key1", "diff1_key2", "diff1_key3"
    )
    assert result == 1

    # Bits 3 and 4 are in Y keys but not in key1
    assert await async_redis.getbit("diff1_dest", 0) == 0
    assert await async_redis.getbit("diff1_dest", 3) == 1
    assert await async_redis.getbit("diff1_dest", 4) == 1


@mark.asyncio
async def test_andor(async_redis: Redis) -> None:
    """Test BITOP ANDOR operation - bit set if in X and in one or more Y"""
    # Setup test keys with known bit patterns
    await async_redis.setbit("andor_key1", 0, 1)
    await async_redis.setbit("andor_key1", 1, 1)
    await async_redis.setbit("andor_key1", 2, 1)

    await async_redis.setbit("andor_key2", 1, 1)
    await async_redis.setbit("andor_key2", 2, 1)
    await async_redis.setbit("andor_key2", 3, 1)

    await async_redis.setbit("andor_key3", 2, 1)
    await async_redis.setbit("andor_key3", 3, 1)
    await async_redis.setbit("andor_key3", 4, 1)

    result = await async_redis.bitop(
        "ANDOR", "andor_dest", "andor_key1", "andor_key2", "andor_key3"
    )
    assert result == 1

    # Bit must be in key1 AND (key2 OR key3)
    assert (
        await async_redis.getbit("andor_dest", 0) == 0
    )  # In key1 but not in key2 or key3
    assert await async_redis.getbit("andor_dest", 1) == 1  # In key1 and key2
    assert await async_redis.getbit("andor_dest", 2) == 1  # In key1 and both key2, key3


@mark.asyncio
async def test_one(async_redis: Redis) -> None:
    """Test BITOP ONE operation - bit set if in exactly one source"""
    # Setup test keys with known bit patterns
    await async_redis.setbit("one_key1", 0, 1)
    await async_redis.setbit("one_key1", 1, 1)
    await async_redis.setbit("one_key1", 2, 1)

    await async_redis.setbit("one_key2", 1, 1)
    await async_redis.setbit("one_key2", 2, 1)
    await async_redis.setbit("one_key2", 3, 1)

    await async_redis.setbit("one_key3", 2, 1)
    await async_redis.setbit("one_key3", 3, 1)
    await async_redis.setbit("one_key3", 4, 1)

    result = await async_redis.bitop(
        "ONE", "one_dest", "one_key1", "one_key2", "one_key3"
    )
    assert result == 1

    # Bits must be in exactly one source
    assert await async_redis.getbit("one_dest", 0) == 1  # Only in key1
    assert await async_redis.getbit("one_dest", 1) == 0  # In key1 and key2
    assert await async_redis.getbit("one_dest", 2) == 0  # In all three
    assert await async_redis.getbit("one_dest", 4) == 1  # Only in key3


@mark.asyncio
async def test_diff_requires_multiple_keys(async_redis: Redis) -> None:
    """Test BITOP DIFF requires at least two source keys"""
    with raises(Exception) as exception:
        await async_redis.bitop("DIFF", "dest", "key1")

    assert "BITOP DIFF must be called with at least two source keys" in str(
        exception.value
    )


@mark.asyncio
async def test_diff1_requires_multiple_keys(async_redis: Redis) -> None:
    """Test BITOP DIFF1 requires at least two source keys"""
    with raises(Exception) as exception:
        await async_redis.bitop("DIFF1", "dest", "key1")

    assert "BITOP DIFF1 must be called with at least two source keys" in str(
        exception.value
    )


@mark.asyncio
async def test_andor_requires_multiple_keys(async_redis: Redis) -> None:
    """Test BITOP ANDOR requires at least two source keys"""
    with raises(Exception) as exception:
        await async_redis.bitop("ANDOR", "dest", "key1")

    assert "BITOP ANDOR must be called with at least two source keys" in str(
        exception.value
    )
