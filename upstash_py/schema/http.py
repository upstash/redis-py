from typing import TypedDict


# The type of the "result" field returned by the REST API.
RESTResult = str | int | list | dict | None

"""
 The type of encoding that will be passed as a header to the REST API. 
 If set to False, no encoding will be used.
 """
RESTEncoding = str | bool


class RESTResponse(TypedDict):
    """
    The types of the fields that can be returned by the REST API.
    """

    result: str | int | dict | None
    error: str
