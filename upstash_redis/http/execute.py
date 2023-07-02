import aiohttp
from upstash_redis.exception import UpstashException
from upstash_redis.http.decode import decode
from upstash_redis.schema.http import RESTResult, RESTResponse, RESTEncoding
from upstash_redis.schema.telemetry import TelemetryData
from asyncio import sleep
from aiohttp import ClientSession
from json import dumps
from platform import python_version
from typing import Union, List, Dict

from requests import Session
import time


async def async_execute(
    session: ClientSession,
    url: str,
    token: str,
    encoding: RESTEncoding,
    retries: int,
    retry_interval: int,
    command: List,
    allow_telemetry: bool,
    telemetry_data: Union[TelemetryData, None] = None,
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
        element if (isinstance(element, str) or isinstance(element, int) or isinstance(element, float)) else dumps(element)
        for element in command
    ]

    for i in range(retries + 1):
        try:
            headers: Dict[str, str] = {"Authorization": f"Bearer {token}"}

            if allow_telemetry:
                if telemetry_data:
                    # Avoid the [] syntax to prevent KeyError from being raised.
                    if telemetry_data.get("runtime"):
                        headers["Upstash-Telemetry-Runtime"] = telemetry_data["runtime"]
                    else:
                        headers[
                            "Upstash-Telemetry-Runtime"
                        ] = f"python@v{python_version()}"

                    if telemetry_data.get("sdk"):
                        headers["Upstash-Telemetry-Sdk"] = telemetry_data["sdk"]
                    else:
                        headers["Upstash-Telemetry-Sdk"] = "upstash_redis@development"

                    if telemetry_data.get("platform"):
                        headers["Upstash-Telemetry-Platform"] = telemetry_data[
                            "platform"
                        ]

            if encoding:
                headers["Upstash-Encoding"] = encoding

            async with session.post(url, headers=headers, json=command) as response:
                body: RESTResponse = await response.json()

                # Avoid the [] syntax to prevent KeyError from being raised.
                if body.get("error"):
                    raise UpstashException(body.get("error"))

                return (
                    decode(raw=body["result"], encoding=encoding)
                    if encoding
                    else body["result"]
                )
            break
        except Exception as exception:
            if i == retries:
                # If we exhausted all the retries, raise the exception.
                raise exception
            else:
                await sleep(retry_interval)


def sync_execute(
    session: Session,
    url: str,
    token: str,
    encoding: RESTEncoding,
    retries: int,
    retry_interval: int,
    command: List,
    allow_telemetry: bool,
    telemetry_data: Union[TelemetryData, None] = None,
) -> RESTResult:

    command = [
        element if (isinstance(element, str) or isinstance(element, int) or isinstance(element, float)) else dumps(element)
        for element in command
    ]

    for i in range(retries + 1):
        try:
            headers: Dict[str, str] = {"Authorization": f"Bearer {token}"}

            if allow_telemetry:
                if telemetry_data:
                    # Avoid the [] syntax to prevent KeyError from being raised.
                    if telemetry_data.get("runtime"):
                        headers["Upstash-Telemetry-Runtime"] = telemetry_data["runtime"]
                    else:
                        headers[
                            "Upstash-Telemetry-Runtime"
                        ] = f"python@v{python_version()}"

                    if telemetry_data.get("sdk"):
                        headers["Upstash-Telemetry-Sdk"] = telemetry_data["sdk"]
                    else:
                        headers["Upstash-Telemetry-Sdk"] = "upstash_redis@development"

                    if telemetry_data.get("platform"):
                        headers["Upstash-Telemetry-Platform"] = telemetry_data[
                            "platform"
                        ]

            if encoding:
                headers["Upstash-Encoding"] = encoding

            response = session.post(url, headers=headers, json=command).json()

            # Avoid the [] syntax to prevent KeyError from being raised.
            if response.get("error"):
                raise UpstashException(response.get("error"))

            return (
                decode(raw=response["result"], encoding=encoding)
                if encoding
                else response["result"]
            )
        except Exception as exception:
            if i == retries:
                # If we exhausted all the retries, raise the exception.
                raise exception
            else:
                time.sleep(retry_interval)
