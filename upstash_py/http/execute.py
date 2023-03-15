from upstash_py.exception import UpstashException
from time import sleep
from upstash_py.schema import RESTResult, RESTResponse, RESTEncoding
from upstash_py.utils.base import base64_to_string
from aiohttp import ClientSession


def decode(raw: RESTResult, encoding: str) -> RESTResult:
    """
    Decode the response received from the REST API
    """
    if encoding == "base64":
        if isinstance(raw, str):
            return "OK" if raw == "OK" else base64_to_string(raw)

        # We need to use "type(None)" instead of simply "None" to evaluate properly
        elif isinstance(raw, (int, type(None))):
            return raw

        elif isinstance(raw, list):
            # TODO add pipeline support
            return list(
                map(
                    # "element" could also be None
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
    command: str
) -> RESTResult:
    """
    Execute the given command over the REST API
    """

    exception: Exception | None = None

    # Encode the command to be attached to the URL
    command = command.replace(" ", "/")

    for i in range(retries):
        try:
            headers: dict[str, str] = {"Upstash-Encoding": encoding} if encoding else {}

            async with session.get(f'{url}/{command}?_token={token}', headers=headers) as response:
                body: RESTResponse = await response.json()
                # Avoid the [] syntax to prevent KeyError from being raised
                if body.get("error"):
                    raise UpstashException(body.get("error"))

                return decode(raw=body.get("result"), encoding=encoding) if encoding else body.get("result")
            break
        except Exception as _exception:
            exception = _exception
            sleep(retry_interval)

    raise UpstashException(str(exception))
