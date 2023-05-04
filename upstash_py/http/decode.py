from upstash_py.schema.http import RESTResult
from upstash_py.utils.base import base64_to_string
from upstash_py.exception import UpstashException


def decode(raw: RESTResult, encoding: str) -> RESTResult:
    """
    Decode the response received from the REST API.
    """

    if encoding == "base64":
        if isinstance(raw, str):
            return "OK" if raw == "OK" else base64_to_string(raw)

        elif isinstance(raw, int) or raw is None:
            return raw

        elif isinstance(raw, list):
            return [
                # Decode recursively.
                decode(element, encoding) for element in raw
            ]
        else:
            raise UpstashException(f'Error decoding data for result type {str(type(raw))}')
