from upstash_py.schema.http import RESTResult
from aiohttp import ClientSession
from os import environ

url: str = environ["UPSTASH_REDIS_REST_URL"]
token: str = environ["UPSTASH_REDIS_REST_TOKEN"]

headers: dict[str, str] = {"Authorization": f'Bearer {token}'}


async def execute_on_http(*command_elements: str) -> RESTResult:
    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=[*command_elements]) as response:
            if response.status != 200:
                raise Exception(f'Command failed to execute: {[*command_elements]}')

            return (await response.json())["result"]
