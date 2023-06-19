from typing import TypedDict, Literal, TypeVar, Any, Union, List


"""
The type of the "result" field returned by the REST API. 

It is a TypeVar mainly because of two reasons:
 - the "decode" function, which should return the same data type that was passed via the "raw" parameter.
 - the "execute" function's return is not an Union.
"""
RESTResult = TypeVar("RESTResult", str, int, List, None, Literal[0, 1], Literal["OK"])

"""
The type of encoding that will be passed as a header to the REST API. 
If set to False, no encoding will be used.
"""
RESTEncoding = Union[Literal["base64"], Literal[False]]


class RESTResponse(TypedDict):
    """
    The types of the fields that can be returned by the REST API.
    """

    result: Any
    error: str
