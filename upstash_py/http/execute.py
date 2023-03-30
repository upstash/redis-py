from upstash_py.exception import UpstashException
from asyncio import sleep
from upstash_py.schema.http import RESTResult, RESTResponse, RESTEncoding
from upstash_py.utils.base import base64_to_string
from aiohttp import ClientSession
from json import dumps


def decode(raw: RESTResult, encoding: str) -> RESTResult:
    """
    Decode the response received from the REST API.
    """

    if encoding == "base64":
        if isinstance(raw, str):
            return "OK" if raw == "OK" else base64_to_string(raw)

        # We need to use "type(None)" instead of simply "None" to evaluate properly.
        elif isinstance(raw, (int, type(None))):
            return raw

        elif isinstance(raw, list):
            # TODO add pipeline support
            return [
               # If the element is a string, decode it.
               base64_to_string(element) if isinstance(element, str)

               # If it's a list, decode each string in it
               else [
                   base64_to_string(member) if isinstance(member, str)

                   else raw

                   for member in element
               ] if isinstance(element, list)

               else element

               for element in raw
            ]
        else:
            raise UpstashException(f'Error decoding data for result type {str(type(raw))}')


async def execute(
    session: ClientSession,
    url: str,
    token: str,
    encoding: RESTEncoding,
    retries: int,
    retry_interval: int,
    command: list
) -> RESTResult:
    """
    Execute the given command over the REST API.
    """

    # Serialize the command; more specifically, write string-incompatible types as JSON strings.
    command = [
        element if isinstance(element, str | int | float)

        else dumps(element)

        for element in command
    ]

    for i in range(retries + 1):
        try:
            headers: dict[str, str] = {"Authorization": f'Bearer {token}'}

            if encoding:
                headers["Upstash-Encoding"] = encoding

            async with session.post(url, headers=headers, json=command) as response:
                body: RESTResponse = await response.json()
                # Avoid the [] syntax to prevent KeyError from being raised.
                if body.get("error"):
                    raise UpstashException(body.get("error"))

                return decode(raw=body.get("result"), encoding=encoding) if encoding else body.get("result")
            break
        except Exception as exception:
            if i == retries:
                # If we exhausted all the retries, raise the exception.
                raise exception
            else:
                await sleep(retry_interval)
