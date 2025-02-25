from os import environ
from typing import Any, Dict

import httpx

from upstash_redis.typing import RESTResultT

url: str = environ["UPSTASH_REDIS_REST_URL"]
token: str = environ["UPSTASH_REDIS_REST_TOKEN"]

headers: Dict[str, str] = {"Authorization": f"Bearer {token}"}


async def execute_on_http(*command_elements: str) -> RESTResultT:
    async with httpx.AsyncClient(timeout=None) as client:
        response = await client.post(url=url, headers=headers, json=[*command_elements])
        body: Dict[str, Any] = response.json()

        # Avoid the [] syntax to prevent KeyError from being raised.
        if body.get("error"):
            raise Exception(body.get("error"))

        return body["result"]


def sync_execute_on_http(*command_elements: str) -> RESTResultT:
    response = httpx.post(url, headers=headers, json=[*command_elements])
    body: Dict[str, Any] = response.json()

    # Avoid the [] syntax to prevent KeyError from being raised.
    if body.get("error"):
        raise Exception(body.get("error"))

    return body["result"]
