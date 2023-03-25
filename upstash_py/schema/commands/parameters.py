from typing import TypedDict

# str allows for "#" syntax.
BitFieldOffset = int | str

"""
A permissive type for fields and key parameter values (ex: as in Hashes or Strings but not Bit).

Note that it doesn't necessarily represent the actual type that will be stored in Redis.
"""
# TODO check if more types could be allowed.
GeneralAtomicValue = str | int | float


class GeoMember(TypedDict):
    """
    Represents the initial properties of a geo member, usually used with "GEOADD".
    """
    longitude: float
    latitude: float
    name: str
