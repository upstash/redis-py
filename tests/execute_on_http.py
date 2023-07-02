from upstash_redis.schema.http import RESTResult, RESTResponse
from aiohttp import ClientSession
from os import environ
from typing import Dict
import requests

url: str = environ["UPSTASH_REDIS_REST_URL"]
token: str = environ["UPSTASH_REDIS_REST_TOKEN"]

headers: Dict[str, str] = {"Authorization": f"Bearer {token}"}


async def execute_on_http(*command_elements: str) -> RESTResult:
    async with ClientSession() as session:
        async with session.post(
            url=url, headers=headers, json=[*command_elements]
        ) as response:
            body: RESTResponse = await response.json()

            # Avoid the [] syntax to prevent KeyError from being raised.
            if body.get("error"):
                raise Exception(body.get("error"))

            return body["result"]

def sync_execute_on_http(*command_elements: str) -> RESTResult:

    response = requests.post(url, headers=headers, json=[*command_elements])
    body = response.json()

    print(body)

    # Avoid the [] syntax to prevent KeyError from being raised.
    if body.get("error"):
        raise Exception(body.get("error"))

    return body["result"]
        

        