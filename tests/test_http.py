import typing as t
from os import environ
from platform import python_version
from unittest.mock import patch

import pytest
from aiohttp import ClientSession
from pytest import mark, raises
from requests import Session

from upstash_redis.errors import UpstashError
from upstash_redis.http import async_execute, decode, make_headers, sync_execute
from upstash_redis.typing import RESTEncoding


@mark.asyncio
async def test_async_execute_without_encoding() -> None:
    async with ClientSession() as session:
        assert (
            await async_execute(
                session=session,
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(environ["UPSTASH_REDIS_REST_TOKEN"], False, False),
                retries=0,
                retry_interval=0,
                encoding=False,
                command=["SET", "a", "b"],
            )
            == "OK"
        )


@mark.asyncio
async def test_async_execute_with_encoding() -> None:
    async with ClientSession() as session:
        assert (
            await async_execute(
                session=session,
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                retries=0,
                retry_interval=0,
                encoding="base64",
                command=["SET", "a", "b"],
            )
            == "OK"
        )


@mark.asyncio
async def test_async_execute_with_encoding_and_object() -> None:
    async with ClientSession() as session:
        assert (
            await async_execute(
                session=session,
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                retries=0,
                retry_interval=0,
                encoding="base64",
                command=["SET", "a", {"b": "c"}],
            )
            == "OK"
        )


@mark.asyncio
async def test_async_execute_with_invalid_command() -> None:
    async with ClientSession() as session:
        with raises(UpstashError):
            await async_execute(
                session=session,
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                retries=0,
                retry_interval=0,
                encoding="base64",
                # We give one parameter to "SET" instead of two.
                command=["SET", "a"],
            )


def test_sync_execute_without_encoding() -> None:
    with Session() as session:
        assert (
            sync_execute(
                session=session,
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(environ["UPSTASH_REDIS_REST_TOKEN"], False, False),
                retries=0,
                retry_interval=0,
                encoding=False,
                command=["SET", "a", "b"],
            )
            == "OK"
        )


def test_sync_execute_with_encoding() -> None:
    with Session() as session:
        assert (
            sync_execute(
                session=session,
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                retries=0,
                retry_interval=0,
                encoding="base64",
                command=["SET", "a", "b"],
            )
            == "OK"
        )


def test_sync_execute_with_encoding_and_object() -> None:
    with Session() as session:
        assert (
            sync_execute(
                session=session,
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                retries=0,
                retry_interval=0,
                encoding="base64",
                command=["SET", "a", {"b": "c"}],
            )
            == "OK"
        )


def test_sync_execute_with_invalid_command() -> None:
    with Session() as session:
        with raises(UpstashError):
            sync_execute(
                session=session,
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                retries=0,
                retry_interval=0,
                encoding="base64",
                # We give one parameter to "SET" instead of two.
                command=["SET", "a"],
            )


@pytest.mark.parametrize(
    "arg,expected",
    [
        ("dGVzdA==", "test"),
        ("OK", "OK"),
        (1, 1),
        (None, None),
        (["YWJjZA==", 1, "MQ==", None], ["abcd", 1, "1", None]),
        (["YQ==", ["YWJjZA==", 1]], ["a", ["abcd", 1]]),
        ([None, ["MQ==", [1, "MQ=="]]], [None, ["1", [1, "1"]]]),
    ],
    ids=["simple string", "ok", "integer", "none", "list", "2d list", "3d list"],
)
def test_decode(arg: t.Any, expected: t.Any) -> None:
    assert decode(arg) == expected


@pytest.mark.parametrize(
    "token,encoding,allow_telemetry,expected",
    [
        ("token", False, False, {"Authorization": "Bearer token"}),
        (
            "token",
            "base64",
            False,
            {"Authorization": "Bearer token", "Upstash-Encoding": "base64"},
        ),
        (
            "token",
            False,
            True,
            {
                "Authorization": "Bearer token",
                "Upstash-Telemetry-Sdk": "upstash_redis@python",
                "Upstash-Telemetry-Runtime": f"python@v{python_version()}",
                "Upstash-Telemetry-Platform": "unknown",
            },
        ),
        (
            "token",
            "base64",
            True,
            {
                "Authorization": "Bearer token",
                "Upstash-Encoding": "base64",
                "Upstash-Telemetry-Sdk": "upstash_redis@python",
                "Upstash-Telemetry-Runtime": f"python@v{python_version()}",
                "Upstash-Telemetry-Platform": "unknown",
            },
        ),
    ],
    ids=[
        "only with token",
        "with encoding",
        "with telemetry",
        "with encoding and telemetry",
    ],
)
def test_make_headers(
    token: str,
    encoding: RESTEncoding,
    allow_telemetry: bool,
    expected: t.Dict[str, str],
) -> None:
    # Make sure that we use "unknown" for the platform no matter where the test is run
    with patch("os.getenv", return_value=None):
        assert make_headers(token, encoding, allow_telemetry) == expected


def test_make_headers_on_vercel() -> None:
    with patch("os.getenv", side_effect=lambda arg: arg if arg == "VERCEL" else None):
        assert make_headers("token", False, True) == {
            "Authorization": "Bearer token",
            "Upstash-Telemetry-Sdk": "upstash_redis@python",
            "Upstash-Telemetry-Runtime": f"python@v{python_version()}",
            "Upstash-Telemetry-Platform": "vercel",
        }


def test_make_headers_on_aws() -> None:
    with patch(
        "os.getenv", side_effect=lambda arg: arg if arg == "AWS_REGION" else None
    ):
        assert make_headers("token", False, True) == {
            "Authorization": "Bearer token",
            "Upstash-Telemetry-Sdk": "upstash_redis@python",
            "Upstash-Telemetry-Runtime": f"python@v{python_version()}",
            "Upstash-Telemetry-Platform": "aws",
        }
