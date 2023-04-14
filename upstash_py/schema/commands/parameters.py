from typing import TypedDict

# "str" allows for "#" syntax.
BitFieldOffset = int | str


class GeoMember(TypedDict):
    """
    Represents the initial properties of a geo member, usually used with "GEOADD".
    """
    longitude: float
    latitude: float
    name: str


# "str" allows for "-inf" and "+inf". Not to be confused with the lexical min and max type (which is "str").
FloatMinMax = float | str
