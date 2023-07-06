from os import environ
from typing import Any, Dict

import requests
from aiohttp import ClientSession

from upstash_redis.typing import RESTResultT

url: str = environ["UPSTASH_REDIS_REST_URL"]
token: str = environ["UPSTASH_REDIS_REST_TOKEN"]

headers: Dict[str, str] = {"Authorization": f"Bearer {token}"}


async def execute_on_http(*command_elements: str) -> RESTResultT:
    async with ClientSession() as session:
        async with session.post(
            url=url, headers=headers, json=[*command_elements]
        ) as response:
            body: Dict[str, Any] = await response.json()

            # Avoid the [] syntax to prevent KeyError from being raised.
            if body.get("error"):
                raise Exception(body.get("error"))

            return body["result"]


def sync_execute_on_http(*command_elements: str) -> RESTResultT:
    response = requests.post(url, headers=headers, json=[*command_elements])
    body: Dict[str, Any] = response.json()

    # Avoid the [] syntax to prevent KeyError from being raised.
    if body.get("error"):
        raise Exception(body.get("error"))

    return body["result"]
