from typing import Callable, Dict, List, Literal, Optional, Tuple, Union

from upstash_redis.utils import GeoSearchResult


def list_to_dict(raw: List, command=None) -> Dict:
    """
    Convert a list that contains ungrouped pairs as consecutive elements (usually field-value or similar) into a dict.
    """

    return {raw[iterator]: raw[iterator + 1] for iterator in range(0, len(raw), 2)}


def format_geopos(
    raw: List[Optional[List[str]]], command=None
) -> List[Union[Tuple[float, float], None]]:
    return [
        (float(member[0]), float(member[1])) if isinstance(member, List) else None
        for member in raw
    ]


def format_geo_search_result(
    raw: List[List[Union[str, List[str]]]],
    with_distance: bool,
    with_hash: bool,
    with_coordinates: bool,
) -> List[GeoSearchResult]:
    """
    Format the raw output given by some Geo commands, usually the ones that return properties of members,
    when additional properties are requested.

    They generally return, if requested, in order:
     - the distance (float)
     - the hash (int)
     - the coordinates as:
        - the longitude
        - the latitude

    All represented as strings
    """

    results: List[GeoSearchResult] = []

    # (metin): mypy has trouble narrowing down the types in this function.
    # We can use typing.cast(str, xxx) to force it manually narrow
    # down the types, but I don't like the idea of calling a function
    # that has a runtime cost to just please the type checker. Until
    # there is a better way of doing it, I wil just ignore the errors.

    for member in raw:
        result = GeoSearchResult(member[0])  # type: ignore[arg-type]

        if with_distance:
            result.distance = float(member[1])  # type: ignore[arg-type]

            if with_hash:
                result.hash = int(member[2])  # type: ignore[arg-type]

                if with_coordinates:
                    result.longitude = float(member[3][0])  # type: ignore[arg-type]
                    result.latitude = float(member[3][1])  # type: ignore[arg-type]

            elif with_coordinates:
                result.longitude = float(member[2][0])  # type: ignore[arg-type]
                result.latitude = float(member[2][1])  # type: ignore[arg-type]

        elif with_hash:
            result.hash = int(member[1])  # type: ignore[arg-type]

            if with_coordinates:
                result.longitude = float(member[2][0])  # type: ignore[arg-type]
                result.latitude = float(member[2][1])  # type: ignore[arg-type]

        elif with_coordinates:
            result.longitude = float(member[1][0])  # type: ignore[arg-type]
            result.latitude = float(member[1][1])  # type: ignore[arg-type]

        results.append(result)

    return results


def format_hash_return(raw: List[str], command=None) -> Dict[str, str]:
    """
    Format the raw output given by Hash commands, usually the ones that return the field-value
    pairs of Hashes.
    """

    return list_to_dict(raw=raw)


def format_pubsub_numsub_return(raw: List[Union[str, int]]) -> Dict[str, int]:
    """
    Format the raw output returned by "PUBSUB NUMSUB".
    """

    return list_to_dict(raw=raw)


def format_bool_list(raw: List[Literal[0, 1]]) -> List[bool]:
    """
    Format a list of boolean integers.
    """

    return [bool(value) for value in raw]


def format_time(raw: List[str], command=None) -> Tuple[int, int]:
    return (int(raw[0]), int(raw[1]))


def format_sorted_set_return(raw: List[str], command=None) -> List[Tuple[str, float]]:
    """
    Format the raw output given by Sorted Set commands, usually the ones that return the member-score
    pairs of Sorted Sets.
    """
    it = iter(raw)
    return list(zip(it, map(float, it)))


def format_float_list(raw: List[Optional[str]], command=None) -> List[Optional[float]]:
    """
    Format a list of strings representing floats or None values.
    """

    return [float(value) if value is not None else None for value in raw]


def set_formatter(res, command):
    options = command[3:]

    if "GET" in options:
        return res
    return res == "OK"


def ok_to_bool(res, command):
    return res == "OK"


def to_bool(res, command):
    return bool(res)


def list_to_bool_list(res, command):
    return list(map(bool, res))


def float_or_none(res, command):
    if res is None:
        return None

    return float(res)


def to_float(res, command):
    return float(res)


def format_scan(res, command):
    return (int(res[0]), res[1])


def hscan_formatter(res, command):
    return [int(res[0]), format_hash_return(res[1])]


def sscan_formatter(res, command):  ## same with scan_formatter
    return [int(res[0]), res[1]]


def zscan_formatter(res, command):  ## same with scan_formatter
    return [int(res[0]), format_sorted_set_return(res[1])]


def zscore_formatter(res, command):
    return float(res) if res is not None else res


def format_search(res, command):
    withdist = "WITHDIST" in command
    withhash = "WITHHASH" in command
    withcoord = "WITHCOORD" in command
    if withdist or withhash or withcoord:
        return format_geo_search_result(res, withdist, withhash, withcoord)

    return res


def format_hrandfield(res, command):
    withvalues = "WITHVALUES" in command
    if withvalues:
        return format_hash_return(res)

    return res


def zadd_formatter(res, command):
    incr = "INCR" in command
    if incr:
        return float(res) if res is not None else res

    return res


def format_zdiff(res, command):
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


FORMATTERS: Dict[str, Callable] = {
    "COPY": to_bool,
    "EXPIRE": to_bool,
    "EXPIREAT": to_bool,
    "PERSIST": to_bool,
    "PEXPIRE": to_bool,
    "PEXPIREAT": to_bool,
    "RENAME": ok_to_bool,
    "RENAMENX": to_bool,
    "SCAN": format_scan,
    "GEODIST": float_or_none,
    "GEOPOS": format_geopos,
    "GEORADIUS": format_search,
    "GEORADIUS_RO": format_search,
    "GEORADIUSBYMEMBER": format_search,
    "GEORADIUSBYMEMBER_RO": format_search,
    "GEOSEARCH": format_search,
    "HEXISTS": to_bool,
    "HGETALL": format_hash_return,
    "HINCRBYFLOAT": to_float,
    "HRANDFIELD": format_hrandfield,
    "HSCAN": hscan_formatter,
    "HSETNX": to_bool,
    "PFADD": to_bool,
    "PFMERGE": ok_to_bool,
    "TIME": format_time,
    "SISMEMBER": to_bool,
    "SMISMEMBER": list_to_bool_list,
    "SMOVE": to_bool,
    "SSCAN": sscan_formatter,
    "ZADD": zadd_formatter,
    "ZDIFF": format_zdiff,
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
    "PUBSUB NUMSUB": list_to_dict,
    "FLUSHALL": ok_to_bool,
    "FLUSHDB": ok_to_bool,
    "PSETEX": ok_to_bool,
    "SET": set_formatter,
    "SETEX": ok_to_bool,
    "SETNX": to_bool,
    "MSET": ok_to_bool,
    "MSETNX": to_bool,
    "HMSET": ok_to_bool,
    "LSET": ok_to_bool,
    "SCRIPT FLUSH": ok_to_bool,
    "SCRIPT EXISTS": list_to_bool_list,
}
