from upstash_py.exception import UpstashException
from upstash_py.config import config
from requests import get, Response
from time import sleep
from upstash_py.schema import UpstashResponse, RESTResponse
from upstash_py.utils.base import base64_to_string


def decode(raw: RESTResponse) -> RESTResponse:
    # Convert the result to string as matching class types is trickier
    match str(type(raw)):
        case "<class 'str'>":
            # return "OK" if result == "OK" else base64_to_string(result)
            return raw
        case "<class 'int'>" | "<class 'NoneType'>":
            return raw
        case "<class 'list'>":
            return raw
        case _:
            raise UpstashException(f'Error decoding data for result type {type(raw)}')


def execute(url: str, token: str, command: str) -> RESTResponse:
    # TODO: allow custom values
    retries = int(config["HTTP_RETRIES"])
    retry_interval = int(config["HTTP_RETRY_INTERVAL"])

    exception: Exception | None = None
    response: Response | None = None

    # Encode the command to be attached to the URL
    command = command.replace(" ", "/")

    for i in range(retries):
        try:
            response = get(f'{url}/{command}?_token={token}')
            break
        except Exception as _exception:
            exception = _exception
            sleep(retry_interval)

    if response is None:
        raise UpstashException(str(exception))

    body: UpstashResponse = response.json()
    # Avoid the [] syntax to prevent KeyError from being raised
    if body.get("error"):
        raise UpstashException(body.get("error"))

    return decode(raw=body.get("result"))



