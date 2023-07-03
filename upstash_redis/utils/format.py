from typing import Literal, Tuple, Union, List, Dict


def _list_to_dict(raw: List, command=None) -> Dict:
    """
    Convert a list that contains ungrouped pairs as consecutive elements (usually field-value or similar) into a dict.
    """

    return {raw[iterator]: raw[iterator + 1] for iterator in range(0, len(raw), 2)}


def format_geo_positions_return(
    raw: List[Union[List[str], None]], command=None
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
    raw: List[Union[str, List[Union[str, List[str]]]]],
    with_distance: Union[bool, None],
    with_hash: Union[bool, None],
    with_coordinates: Union[bool, None],
) -> List[Dict[str, Union[str, float, int]]]:
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

    result: List[Dict[str, Union[str, float, int]]] = []

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


def format_hash_return(raw: List[str], command=None) -> Dict[str, str]:
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


def format_server_time_return(raw: List[str], command=None) -> Dict[str, int]:
    """
    Format the raw output returned by "TIME".
    """

    return {"seconds": int(raw[0]), "microseconds": int(raw[1])}


def format_sorted_set_return(raw: List[str], command=None) -> List[Tuple[str, float]]:
    """
    Format the raw output given by Sorted Set commands, usually the ones that return the member-score
    pairs of Sorted Sets.
    """
    it = iter(raw)
    return list(zip(it, map(float, it)))


def format_float_list(
    raw: List[Union[str, None]], command=None
) -> List[Union[float, None]]:
    """
    Format a list of strings representing floats or None values.
    """

    return [float(value) if value is not None else None for value in raw]


def to_set(res, command):
    return set(res)


def ok_to_bool(res, command):
    return res == "OK"


def to_bool(res, command):
    return bool(res)


def list_to_bool_list(res, command):
    return list(map(bool, res))


def to_float(res, command):
    return float(res)


def scan_formatter(res, command):
    return [int(res[0]), res[1]]


def hscan_formatter(res, command):
    return [int(res[0]), format_hash_return(res[1])]


def sscan_formatter(res, command):  ## same with scan_formatter
    return [int(res[0]), res[1]]


def zscan_formatter(res, command):  ## same with scan_formatter
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
        "GEORADIUS_RO": georadius_formatter,
        "GEORADIUSBYMEMBER": georadius_formatter,
        "GEORADIUSBYMEMBER_RO": georadius_formatter,
        "GEOSEARCH": georadius_formatter,
        "HEXISTS": to_bool,
        "HGETALL": format_hash_return,
        "HINCRBYFLOAT": to_float,
        "HRANDFIELD": hrandfield_formatter,
        "HSCAN": hscan_formatter,
        "HSETNX": to_bool,
        "PFADD": to_bool,
        "TIME": format_server_time_return,
        "SISMEMBER": to_bool,
        "SMISMEMBER": list_to_bool_list,
        "SMOVE": to_bool,
        "SSCAN": sscan_formatter,
        "ZADD": zadd_formatter,
        "ZDIFF": zdiff_formatter,
        "ZINCRBY": to_float,
        "ZINTER": zinter_formatter,
        "ZMSCORE": format_float_list,
        "ZPOPMAX": format_sorted_set_return,
        "ZPOPMIN": format_sorted_set_return,
        "ZRANDMEMBER": zrandmember_formatter,
        "ZRANGE": zrange_formatter,
        "ZRANGEBYSCORE": zrangebyscore_formatter,
        "ZREVRANGE": zrevrange_formatter,
        "ZREVRANGEBYSCORE": zrevrangebyscore_formatter,
        "ZSCAN": zscan_formatter,
        "ZSCORE": zscore_formatter,
        "ZUNION": zunion_formatter,
        "INCRBYFLOAT": to_float,
        "PUBSUB NUMSUB": _list_to_dict,
        "SCRIPT EXISTS": format_bool_list,
        "FLUSHALL": ok_to_bool,
        "FLUSHDB": ok_to_bool,
        "MSETNX": to_bool,
        "PSETEX": ok_to_bool,
        "SET": ok_to_bool,
        "SETEX": ok_to_bool,
        "SETNX": to_bool,
        "HMSET": ok_to_bool,
        "SMEMBERS": to_set,
        "SDIFF": to_set,
        "SINTER": to_set,
        "SUNION": to_set,
        "SCRIPT FLUSH": ok_to_bool,
        "SCRIPT EXISTS": list_to_bool_list,
    }

    # TODO: lots of duplicate formatters. unite them, especially format_sorted_set_return
    # TODO: all formatters should take `command` parameter
