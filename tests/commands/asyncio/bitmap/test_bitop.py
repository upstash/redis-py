from aiohttp import ClientSession
from pytest import mark, raises

from tests.async_client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test_not_not_operation() -> None:
    async with redis:
        assert (
            await redis.bitop(
                "AND",
                "bitop_destination_1",
                "string_as_bitop_source_1",
                "string_as_bitop_source_2",
            )
            == 4
        )

        assert await execute_on_http("GET", "bitop_destination_1") == '!"#$'


@mark.asyncio
async def test_without_source_keys() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.bitop("AND", "bitop_destination_1")

        assert str(exception.value) == "At least one source key must be specified."


@mark.asyncio
async def test_not_with_more_than_one_source_key() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.bitop(
                "NOT",
                "bitop_destination_4",
                "string_as_bitop_source_1",
                "string_as_bitop_source_2",
            )

        assert (
            str(exception.value)
            == 'The "NOT " operation takes only one source key as argument.'
        )


@mark.asyncio
async def test_not() -> None:
    async with redis:
        assert (
            await redis.bitop("NOT", "bitop_destination_4", "string_as_bitop_source_1")
            == 4
        )

        # Manually execute over the REST API because the response is not valid JSON.
        async with ClientSession() as session:
            async with session.post(
                url=redis.url,
                headers={"Authorization": f"Bearer {redis.token}"},
                json=["GET", "bitop_destination_4"],
            ) as response:
                # Prevent Python from interpreting escape characters.
                assert await response.text() == r'{"result":"\x9e\x9d\x9c\x9b"}'
