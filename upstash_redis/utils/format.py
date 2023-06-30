from upstash_redis.schema.commands.returns import (
    GeoMembersReturn,
    FormattedGeoMembersReturn,
    HashReturn,
    FormattedHashReturn,
    SortedSetReturn,
    FormattedSortedSetReturn,
)
from typing import Literal, Union, List, Dict


def _list_to_dict(raw: List) -> Dict:
    """
    Convert a list that contains ungrouped pairs as consecutive elements (usually field-value or similar) into a dict.
    """

    return {raw[iterator]: raw[iterator + 1] for iterator in range(0, len(raw), 2)}


def format_geo_positions_return(
    raw: List[Union[List[str], None]],
) -> List[Union[Dict[str, float], None]]:
    """
    Format the raw output returned by "GEOPOS".
    """

    return [
        {"longitude": float(member[0]), "latitude": float(member[1])}
        if isinstance(member, List)
        else None
        for member in raw
    ]


def format_geo_members_return(
    raw: GeoMembersReturn,
    with_distance: Union[bool, None],
    with_hash: Union[bool, None],
    with_coordinates: Union[bool, None],
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
        formatted_member: Dict[str, Union[str, float, int]] = {"member": member[0]}

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


def format_pubsub_numsub_return(raw: List[Union[str, int]]) -> Dict[str, int]:
    """
    Format the raw output returned by "PUBSUB NUMSUB".
    """

    return _list_to_dict(raw=raw)


def format_bool_list(raw: List[Literal[0, 1]]) -> List[bool]:
    """
    Format a list of boolean integers.
    """

    return [bool(value) for value in raw]


def format_server_time_return(raw: List[str]) -> Dict[str, int]:
    """
    Format the raw output returned by "TIME".
    """

    return {"seconds": int(raw[0]), "microseconds": int(raw[1])}


def format_sorted_set_return(raw: SortedSetReturn) -> FormattedSortedSetReturn:
    """
    Format the raw output given by Sorted Set commands, usually the ones that return the member-score
    pairs of Sorted Sets.
    """

    return _list_to_dict(raw=raw)


def format_float_list(raw: List[Union[str, None]]) -> List[Union[float, None]]:
    """
    Format a list of strings representing floats or None values.
    """

    return [float(value) if value is not None else None for value in raw]



def to_bool(raw):
    return bool(raw)

def to_float(raw):
    return float(raw)

def scan_formatter(raw):
    return [int(raw[0]), raw[1]]

def hscan_formatter(raw):
    return [int(raw[0]), format_hash_return(raw[1])]

def sscan_formatter(raw): ## same with scan_formatter
    return [int(raw[0]), raw[1]]

def zscan_formatter(raw): ## same with scan_formatter
    return [int(raw[0]), format_sorted_set_return(raw[1])] 

def zscore_formatter(raw): ## same with scan_formatter
    return float(raw) if raw is not None else raw

class FormattedResponse:
    FORMATTERS = {
        "COPY": to_bool,
        "EXPIRE": to_bool,
        "EXPIREAT": to_bool,
        "PERSIST": to_bool,
        "PEXPIRE": to_bool,
        "PEXPIREAT": to_bool,
        "RENAMENX": to_bool,
        "SCAN": scan_formatter,
        "GEODIST": to_float,
        "GEOPOS": format_geo_positions_return,
        "GEORADIUS": "ASDASDASDASDASDASDASD",
        "GEORADIUS_RO": "ASDASDASDASDASDASDASD",
        "GEORADIUSBYMEMBER": "ASDASDASDASDASDASDASD",
        "GEORADIUSBYMEMBER_RO": "ASDASDASDASDASDASDASD",
        "GEOSEARCH": "ASDASDASDASDASDASDASD",
        "HEXIST": to_bool, # missing test
        "HGETALL": format_hash_return, # missing test
        "HINCRBYFLOAT": to_float, # missing test
        "HRANDFIELD": "ASDASDASDASDASDASDASD",
        "HSCAN": hscan_formatter, # missing test
        "HSETNX": to_bool, # missing test
        "PFADD": to_bool,
        "TIME": format_server_time_return, # missing test
        "SISMEMBER": to_bool, # missing test
        "SMOVE": to_bool, # missing test
        "SSCAN": sscan_formatter, # missing test
        "ZADD": "ASDASDASDASDASDASDASD",
        "ZDIFF": "ASDASDASDASDASDASDASD",
        "ZINCRBY": to_float, # missing test
        "ZINTER": "ASDASDASDASDASDASDASD",
        "ZMSCORE": format_float_list, # missing test
        "ZPOPMAX": format_sorted_set_return, # missing test
        "ZPOPMIN": format_sorted_set_return, # missing test
        "ZRANDMEMBER": "ASDASDASDASDASDASDASD",
        "ZRANGE": "ASDASDASDASDASDASDASD",
        "ZRANGEBYSCORE": "ASDASDASDASDASDASDASD",
        "ZREVRANGE": "ASDASDASDASDASDASDASD",
        "ZREVRANGEBYSCORE": "ASDASDASDASDASDASDASD",
        "ZSCAN": zscan_formatter, # missing test
        "ZSCORE": zscore_formatter, # missing test
        "ZUNION": "ASDASDASDASDASDASDASD",
        "INCRBYFLOAT": to_float, # missing test
        "PUBSUB NUMSUB": _list_to_dict,
        "SCRIPT EXISTS": format_bool_list, # missing test

    }

    # TODO: Check return_cursor stuff.