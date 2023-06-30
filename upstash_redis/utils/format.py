from upstash_redis.schema.commands.returns import (
    GeoMembersReturn,
    FormattedGeoMembersReturn,
    HashReturn,
    FormattedHashReturn,
    SortedSetReturn,
    FormattedSortedSetReturn,
)
from typing import Literal, Union, List, Dict


def _list_to_dict(raw: List, command=None) -> Dict:
    """
    Convert a list that contains ungrouped pairs as consecutive elements (usually field-value or similar) into a dict.
    """

    return {raw[iterator]: raw[iterator + 1] for iterator in range(0, len(raw), 2)}


def format_geo_positions_return(
    raw: List[Union[List[str], None]],
    command = None
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



def to_bool(res, command):
    return bool(res)

def to_float(res, command):
    return float(res)

def scan_formatter(res, command):
    return [int(res[0]), res[1]]

def hscan_formatter(res, command):
    return [int(res[0]), format_hash_return(res[1])]

def sscan_formatter(res, command): ## same with scan_formatter
    return [int(res[0]), res[1]]

def zscan_formatter(res, command): ## same with scan_formatter
    return [int(res[0]), format_sorted_set_return(res[1])] 

def zscore_formatter(res, command):
    return float(res) if res is not None else res

def georadius_formatter(res, command):
    withdist = "WITHDIST" in command 
    withhash = "WITHHASH" in command
    withcoord = "WITHCOORD" in command
    if withdist or withhash or withcoord:
        return format_geo_members_return(res, withdist, withhash, withcoord)

    return res

def hrandfield_formatter(res, command):
    withvalues = "WITHVALUES" in command
    if withvalues:
        return format_hash_return(res)

    return res

def zadd_formatter(res, command):
    incr = "INCR" in command
    if incr:
        return float(res) if res is not None else res

    return res

def zdiff_formatter(res, command):
    withscores = "WITHSCORES" in command
    if withscores:
        return format_sorted_set_return(res)
    
    return res

def zinter_formatter(res, command):
    withscores = "WITHSCORES" in command
    if withscores:
        return format_sorted_set_return(res)
    
    return res

def zrandmember_formatter(res, command):
    withscores = "WITHSCORES" in command
    if withscores:
        return format_sorted_set_return(res)
    
    return res

def zrange_formatter(res, command):
    withscores = "WITHSCORES" in command
    if withscores:
        return format_sorted_set_return(res)
    
    return res

def zrangebyscore_formatter(res, command):
    withscores = "WITHSCORES" in command
    if withscores:
        return format_sorted_set_return(res)
    
    return res

def zrevrange_formatter(res, command):
    withscores = "WITHSCORES" in command
    if withscores:
        return format_sorted_set_return(res)
    
    return res

def zrevrangebyscore_formatter(res, command):
    withscores = "WITHSCORES" in command
    if withscores:
        return format_sorted_set_return(res)
    
    return res

def zunion_formatter(res, command):
    withscores = "WITHSCORES" in command
    if withscores:
        return format_sorted_set_return(res)
    
    return res
    
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
        "GEORADIUS": georadius_formatter,
        "GEORADIUS_RO": georadius_formatter, # same with the georadius, missing tests
        "GEORADIUSBYMEMBER": georadius_formatter, # same with the georadius, missing tests
        "GEORADIUSBYMEMBER_RO": georadius_formatter, # same with the georadius, missing tests
        "GEOSEARCH": georadius_formatter, # same with the georadius, missing tests
        "HEXIST": to_bool, # missing test
        "HGETALL": format_hash_return, # missing test
        "HINCRBYFLOAT": to_float, # missing test
        "HRANDFIELD": hrandfield_formatter, # missing test
        "HSCAN": hscan_formatter, # missing test
        "HSETNX": to_bool, # missing test
        "PFADD": to_bool,
        "TIME": format_server_time_return, # missing test
        "SISMEMBER": to_bool, # missing test
        "SMOVE": to_bool, # missing test
        "SSCAN": sscan_formatter, # missing test
        "ZADD": zadd_formatter, # missing test
        "ZDIFF": zdiff_formatter, # missing test
        "ZINCRBY": to_float, # missing test
        "ZINTER": zinter_formatter, # missing test
        "ZMSCORE": format_float_list, # missing test
        "ZPOPMAX": format_sorted_set_return, # missing test
        "ZPOPMIN": format_sorted_set_return, # missing test
        "ZRANDMEMBER": zrandmember_formatter, # missing test
        "ZRANGE": zrange_formatter, # missing test
        "ZRANGEBYSCORE": zrangebyscore_formatter, # missing test
        "ZREVRANGE": zrevrange_formatter, # missing test
        "ZREVRANGEBYSCORE": zrevrangebyscore_formatter, # missing test
        "ZSCAN": zscan_formatter, # missing test
        "ZSCORE": zscore_formatter, # missing test
        "ZUNION": zunion_formatter, # missing test
        "INCRBYFLOAT": to_float, # missing test
        "PUBSUB NUMSUB": _list_to_dict,
        "SCRIPT EXISTS": format_bool_list, # missing test

    }

    # TODO: Check return_cursor stuff.
    # TODO: lots of duplicate formatters. unite them