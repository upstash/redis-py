from typing import TypedDict, Literal, TypeVar, Generic


"""
The type of the "result" field returned by the REST API. 

It is a TypeVar mainly because of two reasons:
 - the "decode" function, which should return the same data type that was passed via the "raw" parameter.
 - the "execute" function's return is not an Union.
"""
RESTResult = TypeVar(
    "RESTResult",
    str, int, list, None, Literal[0, 1], Literal["OK"]
)

"""
The type of encoding that will be passed as a header to the REST API. 
If set to False, no encoding will be used.
"""
RESTEncoding = str | Literal[False]


class RESTResponse(TypedDict, Generic[RESTResult]):
    """
    The types of the fields that can be returned by the REST API.
    """

    result: RESTResult
    error: str
