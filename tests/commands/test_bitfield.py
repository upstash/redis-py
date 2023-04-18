from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_bitfield_get() -> None:
    async with redis:
        # Test with integer offset.
        assert await redis.bitfield("string").get(encoding="u8", offset=0).execute() == [116]

        # Test with string offset.
        assert await redis.bitfield("string").get(encoding="u8", offset="#1").execute() == [101]


@mark.asyncio
async def test_bitfield_set() -> None:
    """
    Since "SET" returns the old value, the test will check if previous calls set them correctly.
    """

    async with redis:
        async def with_integer_offset() -> list[int]:
            # Test with integer offset.
            return await redis.bitfield("string_for_bitfield_set").set(encoding="u8", offset=0, value=97).execute()

        assert await with_integer_offset() == [116]
        assert await with_integer_offset() == [97]

        async def with_string_offset() -> list[int]:
            # Test with string offset.
            return await redis.bitfield("string_for_bitfield_set").set(encoding="u8", offset="#1", value=115).execute()

        assert await with_string_offset() == [101]
        assert await with_string_offset() == [115]


@mark.asyncio
async def test_bitfield_incrby() -> None:
    """
    Since "INCRBY" returns the new value, the test will check if previous calls set them correctly.
    """

    async with redis:
        async def with_integer_offset() -> list[int]:
            # Test with integer offset.
            return (
                await redis.bitfield("string_for_bitfield_incrby").incrby(encoding="u8", offset=0, increment=1).execute()
            )

        assert await with_integer_offset() == [117]
        assert await with_integer_offset() == [118]

        async def with_string_offset() -> list[int]:
            # Test with string offset.
            return (
                await redis.bitfield("string_for_bitfield_incrby")
                .incrby(encoding="u8", offset="#1", increment=2)
                .execute()
            )

        assert await with_string_offset() == [103]
        assert await with_string_offset() == [105]


@mark.asyncio
async def test_bitfield_chained_commands() -> None:
    """
    Since "SET" returns the old value and "INCRBY" returns the new value,
    the test will check if previous calls set them correctly.
    """

    async with redis:
        async def run_chained_commands() -> list[int]:
            return (
                await redis.bitfield("string_for_bitfield_chained_commands")
                .set(encoding="u8", offset=0, value=97)
                .incrby(encoding="u8", offset=0, increment=1)
                .execute()
            )

        assert await run_chained_commands() == [116, 98]
        assert await run_chained_commands() == [98, 98]


@mark.asyncio
async def test_bitfield_overflow() -> None:
    """
    Since "INCRBY" returns the new value, the test will check if previous calls set them correctly.
    """

    async with redis:
        async def with_sat_overflow() -> list[int]:
            return(
                await redis.bitfield("string_for_bitfield_sat_overflow")
                .incrby(encoding="u8", offset=100, increment=1)
                .overflow("SAT")
                .incrby(encoding="u8", offset=102, increment=1)
                .execute()
            )

        assert await with_sat_overflow() == [1, 5]
        assert await with_sat_overflow() == [2, 10]

        async def with_wrap_overflow() -> list[int]:
            return (
                await redis.bitfield("string_for_bitfield_wrap_overflow")
                .incrby(encoding="u8", offset=100, increment=1)
                .overflow("WRAP")
                .incrby(encoding="u8", offset=102, increment=1)
                .execute()
            )

        assert await with_wrap_overflow() == [1, 5]
        assert await with_wrap_overflow() == [2, 10]

        async def with_fail_overflow() -> list[int]:
            return (
                await redis.bitfield("string_for_bitfield_fail_overflow")
                .incrby(encoding="u8", offset=100, increment=1)
                .overflow("FAIL")
                .incrby(encoding="u8", offset=102, increment=1)
                .execute()
            )

        assert await with_fail_overflow() == [1, 5]
        assert await with_fail_overflow() == [2, 10]
