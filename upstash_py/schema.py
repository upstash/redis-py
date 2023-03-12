from typing import TypedDict


# The type of the "result" field returned by the REST API
RESTResult = str | int | list | None


class RESTResponse(TypedDict):
    """
    The types of the fields that can be returned by the REST API
    """
    result: str | int | dict | None
    error: str
