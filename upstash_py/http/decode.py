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

        # We need to use "type(None)" instead of simply "None" to evaluate properly.
        elif isinstance(raw, (int, type(None))):
            return raw

        elif isinstance(raw, list):
            # TODO add pipeline support
            return [
               # If the element is a string, decode it.
               base64_to_string(element) if isinstance(element, str)

               # If it's a list, decode each string in it.
               else [
                   base64_to_string(member) if isinstance(member, str)

                   else member

                   for member in element
               ] if isinstance(element, list)

               else element

               for element in raw
            ]
        else:
            raise UpstashException(f'Error decoding data for result type {str(type(raw))}')
