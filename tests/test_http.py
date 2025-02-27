from os import environ
from platform import python_version
from typing import Any, Dict, Literal, Optional
from unittest.mock import patch

import pytest
from pytest import mark, raises

from upstash_redis import __version__
from upstash_redis.errors import UpstashError
from upstash_redis.http import decode, make_headers, AsyncHttpClient, SyncHttpClient


@mark.asyncio
async def test_async_execute_without_encoding() -> None:
    async with AsyncHttpClient(
        encoding=None,
        retries=0,
        retry_interval=0,
    ) as client:
        assert (
            await client.execute(
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(environ["UPSTASH_REDIS_REST_TOKEN"], None, False),
                command=["SET", "a", "b"],
            )
            == "OK"
        )


@mark.asyncio
async def test_async_execute_with_encoding() -> None:
    async with AsyncHttpClient(
        encoding="base64",
        retries=0,
        retry_interval=0,
    ) as client:
        assert (
            await client.execute(
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                command=["SET", "a", "b"],
            )
            == "OK"
        )


@mark.asyncio
async def test_async_execute_with_encoding_and_object() -> None:
    async with AsyncHttpClient(
        encoding="base64",
        retries=0,
        retry_interval=0,
    ) as client:
        assert (
            await client.execute(
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                command=["SET", "a", {"b": "c"}],
            )
            == "OK"
        )


@mark.asyncio
async def test_async_execute_with_invalid_command() -> None:
    async with AsyncHttpClient(
        encoding="base64",
        retries=0,
        retry_interval=0,
    ) as client:
        with raises(UpstashError):
            await client.execute(
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                # We give one parameter to "SET" instead of two.
                command=["SET", "a"],
            )


def test_sync_execute_without_encoding() -> None:
    with SyncHttpClient(
        encoding=None,
        retries=0,
        retry_interval=0,
    ) as client:
        assert (
            client.execute(
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(environ["UPSTASH_REDIS_REST_TOKEN"], None, False),
                command=["SET", "a", "b"],
            )
            == "OK"
        )


def test_sync_execute_with_encoding() -> None:
    with SyncHttpClient(
        encoding="base64",
        retries=0,
        retry_interval=0,
    ) as client:
        assert (
            client.execute(
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                command=["SET", "a", "b"],
            )
            == "OK"
        )


def test_sync_execute_with_encoding_and_object() -> None:
    with SyncHttpClient(
        encoding="base64",
        retries=0,
        retry_interval=0,
    ) as client:
        assert (
            client.execute(
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
                command=["SET", "a", {"b": "c"}],
            )
            == "OK"
        )


def test_sync_execute_with_invalid_command() -> None:
    with SyncHttpClient(
        encoding="base64",
        retries=0,
        retry_interval=0,
    ) as client:
        with raises(UpstashError):
            client.execute(
                url=environ["UPSTASH_REDIS_REST_URL"],
                headers=make_headers(
                    environ["UPSTASH_REDIS_REST_TOKEN"], "base64", False
                ),
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
def test_decode(arg: Any, expected: Any) -> None:
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
                "Upstash-Telemetry-Sdk": f"py-upstash-redis@v{__version__}",
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
                "Upstash-Telemetry-Sdk": f"py-upstash-redis@v{__version__}",
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
    encoding: Optional[Literal["base64"]],
    allow_telemetry: bool,
    expected: Dict[str, str],
) -> None:
    # Make sure that we use "unknown" for the platform no matter where the test is run
    with patch("os.getenv", return_value=None):
        assert make_headers(token, encoding, allow_telemetry) == expected


def test_make_headers_on_vercel() -> None:
    with patch("os.getenv", side_effect=lambda arg: arg if arg == "VERCEL" else None):
        assert make_headers("token", None, True) == {
            "Authorization": "Bearer token",
            "Upstash-Telemetry-Sdk": f"py-upstash-redis@v{__version__}",
            "Upstash-Telemetry-Runtime": f"python@v{python_version()}",
            "Upstash-Telemetry-Platform": "vercel",
        }


def test_make_headers_on_aws() -> None:
    with patch(
        "os.getenv", side_effect=lambda arg: arg if arg == "AWS_REGION" else None
    ):
        assert make_headers("token", None, True) == {
            "Authorization": "Bearer token",
            "Upstash-Telemetry-Sdk": f"py-upstash-redis@v{__version__}",
            "Upstash-Telemetry-Runtime": f"python@v{python_version()}",
            "Upstash-Telemetry-Platform": "aws",
        }
