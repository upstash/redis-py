from typing import List, Literal, TypedDict, Union

"""
The type of the "result" field returned by the REST API. 
"""
RESTResultT = Union[str, int, List, None, Literal[0, 1], Literal["OK"]]

# "str" allows for "-inf" and "+inf". Not to be confused with the lexical min and max type (which is "str").
FloatMinMaxT = Union[float, str]


class GeoSearchResult(TypedDict, total=False):
    """
    Represents the result of the geo-search related commands.
    """

    member: str
    distance: float
    hash: int
    longitude: float
    latitude: float
