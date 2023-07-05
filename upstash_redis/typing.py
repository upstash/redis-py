from typing import List, Literal, Optional, TypedDict, Union

"""
The type of the "result" field returned by the REST API. 
"""
RESTResult = Union[str, int, List, None, Literal[0, 1], Literal["OK"]]

"""
The type of encoding that will be passed as a header to the REST API. 
If set to None, no encoding will be used.
"""
RESTEncoding = Optional[Literal["base64"]]

# "str" allows for "#" syntax.
BitFieldOffset = Union[int, str]


class GeoMember(TypedDict):
    """
    Represents the initial properties of a geo member, usually used with "GEOADD".
    """

    longitude: float
    latitude: float
    member: str


# "str" allows for "-inf" and "+inf". Not to be confused with the lexical min and max type (which is "str").
FloatMinMax = Union[float, str]
