import asyncio
import os
import time
from base64 import b64decode
from json import dumps
from platform import python_version
from typing import Any, Dict, List, Literal, Optional, Union, Callable, Type

import httpx

from upstash_redis import __version__
from upstash_redis.errors import UpstashError
from upstash_redis.typing import RESTResultT


def make_headers(
    token: str, encoding: Optional[Literal["base64"]], allow_telemetry: bool
) -> Dict[str, Any]:
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


class SyncHttpClient:
    def __init__(
        self,
        encoding: Optional[Literal["base64"]],
        retries: int,
        retry_interval: float,
        sync_token_cb: Optional[Callable[[str], None]] = None,
    ):
        self._encoding = encoding
        self._retries = retries
        self._retry_interval = retry_interval
        self._sync_token_cb = sync_token_cb
        self._client = httpx.Client(timeout=None)

    def execute(
        self,
        url: str,
        headers: Dict[str, str],
        command: List,
        from_pipeline: bool = False,
    ) -> Union[RESTResultT, List[RESTResultT]]:
        command = _format_command(command, from_pipeline=from_pipeline)

        response: Optional[Dict[str, Any]] = None
        last_error: Optional[Exception] = None

        for attempts_left in range(max(0, self._retries), -1, -1):
            try:
                r = self._client.post(url, headers=headers, json=command)
                sync_token = r.headers.get("Upstash-Sync-Token")
                if self._sync_token_cb and sync_token:
                    self._sync_token_cb(sync_token)

                response = r.json()

                break  # Break the loop as soon as we receive a proper response
            except Exception as e:
                last_error = e

                if attempts_left > 0:
                    time.sleep(self._retry_interval)

        if response is None:
            assert last_error is not None

            # Exhausted all retries, but no response is received
            raise last_error
        if not from_pipeline:
            return format_response(response, self._encoding)
        else:
            return [
                format_response(sub_response, self._encoding)  # type: ignore[arg-type]
                for sub_response in response
            ]

    def close(self):
        self._client.close()

    def __enter__(self) -> "SyncHttpClient":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        self.close()


class AsyncHttpClient:
    def __init__(
        self,
        encoding: Optional[Literal["base64"]],
        retries: int,
        retry_interval: float,
        sync_token_cb: Optional[Callable[[str], None]] = None,
    ):
        self._encoding = encoding
        self._retries = retries
        self._retry_interval = retry_interval
        self._sync_token_cb = sync_token_cb
        self._client = httpx.AsyncClient(timeout=None)

    async def execute(
        self,
        url: str,
        headers: Dict[str, str],
        command: List,
        from_pipeline: bool = False,
    ) -> Union[RESTResultT, List[RESTResultT]]:
        command = _format_command(command, from_pipeline=from_pipeline)

        response: Optional[Union[Dict, List[Dict]]] = None
        last_error: Optional[Exception] = None

        for attempts_left in range(max(0, self._retries), -1, -1):
            try:
                r = await self._client.post(url, headers=headers, json=command)
                sync_token = r.headers.get("Upstash-Sync-Token")

                if self._sync_token_cb and sync_token:
                    self._sync_token_cb(sync_token)

                response = r.json()
                break  # Break the loop as soon as we receive a proper response
            except Exception as e:
                last_error = e

                if attempts_left > 0:
                    await asyncio.sleep(self._retry_interval)

        if response is None:
            assert last_error is not None

            # Exhausted all retries, but no response is received
            raise last_error

        if not from_pipeline:
            return format_response(response, self._encoding)  # type: ignore[arg-type]
        else:
            return [
                format_response(sub_response, self._encoding)
                for sub_response in response
            ]

    async def close(self):
        await self._client.aclose()

    async def __aenter__(self) -> "AsyncHttpClient":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        await self.close()


def format_response(
    response: Dict[str, Any], encoding: Optional[Literal["base64"]]
) -> RESTResultT:
    """
    Raise exception if the response is an error

    Otherwise, decode if an encoding was used
    """
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


def _format_command(command: List[Any], from_pipeline: bool = False):
    """
    Format command

    If not from_pipeline, treat the command as a single command. Otherwise,
    treat it as a list of commands
    """
    if from_pipeline:
        return [
            _format_command(command=pipeline_command, from_pipeline=False)
            for pipeline_command in command
        ]

    return [
        element if isinstance(element, (str, int, float)) else dumps(element)
        for element in command
    ]
