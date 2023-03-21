from typing import TypedDict

# str allows for "#" syntax.
BitFieldOffset = str | int


class GeoMember(TypedDict):
    longitude: float
    latitude: float
    name: str
