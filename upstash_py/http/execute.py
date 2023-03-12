from upstash_py.exception import UpstashException
from requests import get, Response
from time import sleep
from upstash_py.schema import RESTResult, RESTResponse
from upstash_py.utils.base import base64_to_string


def decode(raw: RESTResult) -> RESTResult:
    """
    Decode the response received from the REST API
    """
    match str(type(raw)):
        case "<class 'str'>":
            return "OK" if raw == "OK" else base64_to_string(raw)
        case "<class 'int'>" | "<class 'NoneType'>":
            return raw
        case "<class 'list'>":
            # TODO add pipeline support
            return list(
                map(
                    # "element" could also be None
                    lambda element: base64_to_string(element) if str(type(element)) == "<class 'str'>" else element,
                    raw
                )
            )
        case _:
            raise UpstashException(f'Error decoding data for result type {type(raw)}')


def execute(
    url: str,
    token: str,
    encoding: str,
    retries: int,
    retry_interval: int,
    command: str
) -> RESTResult:
    """
    Execute the given command over the REST API
    """

    exception: Exception | None = None
    response: Response | None = None

    # Encode the command to be attached to the URL
    command = command.replace(" ", "/")

    for i in range(retries):
        try:
            headers: dict[str, str] = {"Upstash-Encoding": encoding} if encoding else {}
            response = get(f'{url}/{command}?_token={token}', headers=headers)
            break
        except Exception as _exception:
            exception = _exception
            sleep(retry_interval)

    if response is None:
        raise UpstashException(str(exception))

    body: RESTResponse = response.json()
    # Avoid the [] syntax to prevent KeyError from being raised
    if body.get("error"):
        raise UpstashException(body.get("error"))

    return decode(raw=body.get("result"))



