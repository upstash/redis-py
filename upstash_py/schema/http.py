from typing import TypedDict, Literal, TypeVar


# The type of the "result" field returned by the REST API.
RESTResult = TypeVar(
    "RESTResult",
    str, int, list, None, Literal[0, 1], Literal["OK"]
)

"""
 The type of encoding that will be passed as a header to the REST API. 
 If set to False, no encoding will be used.
 """
RESTEncoding = str | Literal[False]


class RESTResponse(TypedDict):
    """
    The types of the fields that can be returned by the REST API.
    """

    result: str | int | dict | None
    error: str
