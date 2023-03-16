from upstash_py.exception import UpstashException
from asyncio import sleep
from upstash_py.schema import RESTResult, RESTResponse, RESTEncoding
from upstash_py.utils.base import base64_to_string
from aiohttp import ClientSession


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
            return list(
                map(
                    # "element" could also be None.
                    lambda element: base64_to_string(element) if isinstance(element, str) else element,
                    raw
                )
            )
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
        except Exception as _exception:
            if i == retries:
                # If we exhausted all the retries, raise the exception.
                raise _exception
            else:
                await sleep(retry_interval)
