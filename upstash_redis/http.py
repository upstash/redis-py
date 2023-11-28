import os
import time
from asyncio import sleep
from base64 import b64decode
from json import dumps
from platform import python_version
from typing import Any, Dict, List, Literal, Optional

from aiohttp import ClientSession
from requests import Session

from upstash_redis import __version__
from upstash_redis.errors import UpstashError
from upstash_redis.typing import RESTResultT


def make_headers(
    token: str, encoding: Optional[Literal["base64"]], allow_telemetry: bool
) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
    }

    if encoding == "base64":
        headers["Upstash-Encoding"] = encoding

    if allow_telemetry:
        headers["Upstash-Telemetry-Sdk"] = f"py-upstash-redis@v{__version__}"
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
    encoding: Optional[Literal["base64"]],
    retries: int,
    retry_interval: float,
    command: List,
) -> RESTResultT:
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

    response: Optional[Dict[str, Any]] = None
    last_error: Optional[Exception] = None

    for attempts_left in range(max(0, retries), -1, -1):
        try:
            async with session.post(url, headers=headers, json=command) as r:
                response = await r.json()
                break  # Break the loop as soon as we receive a proper response
        except Exception as e:
            last_error = e

            if attempts_left > 0:
                await sleep(retry_interval)

    if response is None:
        assert last_error is not None

        # Exhausted all retries, but no response is received
        raise last_error

    if response.get("error"):
        raise UpstashError(response["error"])

    result = response["result"]

    if encoding == "base64":
        return decode(result)

    return result


def sync_execute(
    session: Session,
    url: str,
    headers: Dict[str, str],
    encoding: Optional[Literal["base64"]],
    retries: int,
    retry_interval: float,
    command: List[Any],
) -> RESTResultT:
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

    response: Optional[Dict[str, Any]] = None
    last_error: Optional[Exception] = None

    for attempts_left in range(max(0, retries), -1, -1):
        try:
            response = session.post(url, headers=headers, json=command).json()
            break  # Break the loop as soon as we receive a proper response
        except Exception as e:
            last_error = e

            if attempts_left > 0:
                time.sleep(retry_interval)

    if response is None:
        assert last_error is not None

        # Exhausted all retries, but no response is received
        raise last_error

    if response.get("error"):
        raise UpstashError(response["error"])

    result = response["result"]

    if encoding == "base64":
        return decode(result)

    return result


def decode(raw: RESTResultT) -> RESTResultT:
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
