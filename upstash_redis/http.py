import os
import time
from asyncio import sleep
from base64 import b64decode
from json import dumps
from platform import python_version
from typing import Any, Dict, List

from aiohttp import ClientSession
from requests import Session

from upstash_redis.errors import UpstashError
from upstash_redis.typing import RESTEncoding, RESTResult


def make_headers(
    token: str, encoding: RESTEncoding, allow_telemetry: bool
) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
    }

    if encoding == "base64":
        headers["Upstash-Encoding"] = encoding

    if allow_telemetry:
        headers["Upstash-Telemetry-Sdk"] = "upstash_redis@python"
        headers["Upstash-Telemetry-Runtime"] = f"python@v{python_version()}"

        if os.getenv("VERCEL"):
            platform = "vercel"
        elif os.getenv("AWS_REGION"):
            platform = "aws"
        else:
            platform = "unknown"

        headers["Upstash-Telemetry-Platform"] = platform

    return headers


async def async_execute(
    session: ClientSession,
    url: str,
    headers: Dict[str, str],
    encoding: RESTEncoding,
    retries: int,
    retry_interval: int,
    command: List,
) -> RESTResult:
    """
    Execute the given command over the REST API.

    :param encoding: the encoding that can be used by the REST API to parse the response before sending it
    :param retries: how many times an HTTP request will be retried if it fails
    :param retry_interval: how many seconds will be waited between each retry
    :param allow_telemetry: whether anonymous telemetry can be collected
    """

    # Serialize the command; more specifically, write string-incompatible types as JSON strings.
    command = [
        element
        if (
            isinstance(element, str)
            or isinstance(element, int)
            or isinstance(element, float)
        )
        else dumps(element)
        for element in command
    ]

    for i in range(retries + 1):
        try:
            async with session.post(url, headers=headers, json=command) as response:
                body: Dict[str, Any] = await response.json()

                # Avoid the [] syntax to prevent KeyError from being raised.
                if body.get("error"):
                    raise UpstashError(body.get("error"))

                return (
                    decode(raw=body["result"])
                    if encoding == "base64"
                    else body["result"]
                )
        except Exception as exception:
            if i == retries:
                # If we exhausted all the retries, raise the exception.
                raise exception
            else:
                await sleep(retry_interval)


def sync_execute(
    session: Session,
    url: str,
    headers: Dict[str, str],
    encoding: RESTEncoding,
    retries: int,
    retry_interval: int,
    command: List,
) -> RESTResult:
    command = [
        element
        if (
            isinstance(element, str)
            or isinstance(element, int)
            or isinstance(element, float)
        )
        else dumps(element)
        for element in command
    ]

    for i in range(retries + 1):
        try:
            response = session.post(url, headers=headers, json=command).json()

            # Avoid the [] syntax to prevent KeyError from being raised.
            if response.get("error"):
                raise UpstashError(response.get("error"))

            return (
                decode(raw=response["result"])
                if encoding == "base64"
                else response["result"]
            )
        except Exception as exception:
            if i == retries:
                # If we exhausted all the retries, raise the exception.
                raise exception
            else:
                time.sleep(retry_interval)


def decode(raw: RESTResult) -> RESTResult:
    """
    Decode the response received from the REST API.
    """

    if isinstance(raw, str):
        return "OK" if raw == "OK" else b64decode(raw).decode()
    elif isinstance(raw, int) or raw is None:
        return raw
    elif isinstance(raw, List):
        return [
            # Decode recursively.
            decode(element)
            for element in raw
        ]
    else:
        raise UpstashError(f"Error decoding data for result type {str(type(raw))}")
