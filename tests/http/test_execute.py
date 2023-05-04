from upstash_py.http.execute import execute
from upstash_py.exception import UpstashException
from aiohttp import ClientSession
from os import environ
from pytest import raises, mark


@mark.asyncio
async def test_without_encoding() -> None:
    session = ClientSession()

    assert await execute(
        session=session,
        url=environ["UPSTASH_REDIS_REST_URL"],
        token=environ["UPSTASH_REDIS_REST_TOKEN"],
        retries=0,
        retry_interval=0,
        encoding=False,
        command=["SET", "a", "b"]
    ) == "OK"

    await session.close()


@mark.asyncio
async def test_with_encoding() -> None:
    session = ClientSession()

    assert await execute(
        session=session,
        url=environ["UPSTASH_REDIS_REST_URL"],
        token=environ["UPSTASH_REDIS_REST_TOKEN"],
        retries=0,
        retry_interval=0,
        encoding="base64",
        command=["SET", "a", "b"]
    ) == "OK"

    await session.close()


@mark.asyncio
async def test_with_encoding_and_object() -> None:
    session = ClientSession()

    assert await execute(
        session=session,
        url=environ["UPSTASH_REDIS_REST_URL"],
        token=environ["UPSTASH_REDIS_REST_TOKEN"],
        retries=0,
        retry_interval=0,
        encoding="base64",
        command=["SET", "a", {"b": "c"}]
    ) == "OK"

    await session.close()


@mark.asyncio
async def test_with_invalid_command() -> None:
    session = ClientSession()

    with raises(UpstashException):
        await execute(
            session=session,
            url=environ["UPSTASH_REDIS_REST_URL"],
            token=environ["UPSTASH_REDIS_REST_TOKEN"],
            retries=0,
            retry_interval=0,
            encoding="base64",
            # We give one parameter to "SET" instead of two.
            command=["SET", "a"]
        )

    await session.close()
