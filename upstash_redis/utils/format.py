from upstash_redis.schema.commands.returns import (
    GeoMembersReturn,
    FormattedGeoMembersReturn,
    HashReturn,
    FormattedHashReturn,
    SortedSetReturn,
    FormattedSortedSetReturn,
)
from typing import Literal


def _list_to_dict(raw: list) -> dict:
    """
    Convert a list that contains ungrouped pairs as consecutive elements (usually field-value or similar) into a dict.
    """

    return {
        raw[iterator]: raw[iterator + 1]
        for iterator in range(0, len(raw), 2)
    }


def format_geo_positions_return(raw: list[list[str] | None]) -> list[dict[str, float] | None]:
    """
    Format the raw output returned by "GEOPOS".
    """

    return [
        {
            "longitude": float(member[0]),
            "latitude": float(member[1])
            # If the member doesn't exist, GEOPOS will return nil.
        } if isinstance(member, list) else None

        for member in raw
    ]


def format_geo_members_return(
    raw: GeoMembersReturn,
    with_distance: bool | None,
    with_hash: bool | None,
    with_coordinates: bool | None
) -> FormattedGeoMembersReturn:
    """
    Format the raw output given by some Geo commands, usually the ones that return properties of members,
    when additional properties are requested.

    Note that the output's type might differ from the "GeoMember" type that represents the initial properties of
    a geo member.

    They generally return, if requested, in order:
     - the distance (float)
     - the hash (int)
     - the coordinates as:
        - the longitude
        - the latitude

    All represented as strings
    """

    result: FormattedGeoMembersReturn = []

    for member in raw:
        # TODO better type with TypedDict
        formatted_member: dict[str, str | float | int] = {
            "member": member[0]
        }

        if with_distance:
            formatted_member["distance"] = float(member[1])

            if with_hash:
                formatted_member["hash"] = int(member[2])

                if with_coordinates:
                    formatted_member["longitude"] = float(member[3][0])
                    formatted_member["latitude"] = float(member[3][1])

            elif with_coordinates:
                formatted_member["longitude"] = float(member[2][0])
                formatted_member["latitude"] = float(member[2][1])

        elif with_hash:
            formatted_member["hash"] = int(member[1])

            if with_coordinates:
                formatted_member["longitude"] = float(member[2][0])
                formatted_member["latitude"] = float(member[2][1])

        elif with_coordinates:
            formatted_member["longitude"] = float(member[1][0])
            formatted_member["latitude"] = float(member[1][1])

        result.append(formatted_member)

    return result


def format_hash_return(raw: HashReturn) -> FormattedHashReturn:
    """
    Format the raw output given by Hash commands, usually the ones that return the field-value
    pairs of Hashes.
    """

    return _list_to_dict(raw=raw)


def format_pubsub_numsub_return(raw: list[str | int]) -> dict[str, int]:
    """
    Format the raw output returned by "PUBSUB NUMSUB".
    """

    return _list_to_dict(raw=raw)


def format_bool_list(raw: list[Literal[0, 1]]) -> list[bool]:
    """
    Format a list of boolean integers.
    """

    return [bool(value) for value in raw]


def format_server_time_return(raw: list[str]) -> dict[str, int]:
    """
    Format the raw output returned by "TIME".
    """

    return {
        "seconds": int(raw[0]),
        "microseconds": int(raw[1])
    }


def format_sorted_set_return(raw: SortedSetReturn) -> FormattedSortedSetReturn:
    """
    Format the raw output given by Sorted Set commands, usually the ones that return the member-score
    pairs of Sorted Sets.
    """

    return _list_to_dict(raw=raw)


def format_float_list(raw: list[str | None]) -> list[float | None]:
    """
    Format a list of strings representing floats or None values.
    """

    return [
        float(value) if value is not None

        else None

        for value in raw
    ]
