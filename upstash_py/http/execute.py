from upstash_py.exception import UpstashException
from upstash_py.config import config
from requests import get, Response
from time import sleep
from upstash_py.types import UpstashResponse


def execute(url: str, token: str, command: str):
    # TODO: allow custom values
    retries = int(config["HTTP_RETRIES"])
    retry_interval = int(config["HTTP_RETRY_INTERVAL"])

    exception: Exception | None = None
    response: Response | None = None

    for i in range(retries):
        try:
            response = get(url, headers={"Authorization": f'Bearer {token}'})
            break
        except Exception as _exception:
            exception = _exception
            sleep(retry_interval)

    if not response:
        raise UpstashException(exception)

    body: UpstashResponse = response.json()
    # Avoid the [] syntax to prevent KeyError from being raised
    if body.get("error"):
        raise UpstashException(body.get("error"))

    # TODO decode
    return body




