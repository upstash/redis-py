from aiohttp import ClientSession
from pytest import mark, raises

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis


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

    assert await execute_on_http("GET", "bitop_destination_1") == '!"#$'


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
        == 'The "NOT " operation takes only one source key as argument.'
    )


@mark.asyncio
async def test_not(async_redis: Redis) -> None:
    assert (
        await async_redis.bitop(
            "NOT", "bitop_destination_4", "string_as_bitop_source_1"
        )
        == 4
    )

    # Manually execute over the REST API because the response is not valid JSON.
    async with ClientSession() as session:
        async with session.post(
            url=async_redis._url,
            headers={"Authorization": f"Bearer {async_redis._token}"},
            json=["GET", "bitop_destination_4"],
        ) as response:
            # Prevent Python from interpreting escape characters.
            assert await response.text() == r'{"result":"\x9e\x9d\x9c\x9b"}'
