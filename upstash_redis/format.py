from json import loads
from typing import Callable, Dict, List, Optional, Tuple, Union

from upstash_redis.typing import RESTResultT
from upstash_redis.utils import GeoSearchResult


def to_dict(res: List, _) -> Dict:
    """
    Convert a list that contains ungrouped pairs as consecutive elements (usually field-value or similar) into a dict.
    """

    return {res[i]: res[i + 1] for i in range(0, len(res), 2)}


def format_geopos(
    res: List[Optional[List[str]]], _
) -> List[Union[Tuple[float, float], None]]:
    return [
        (float(member[0]), float(member[1])) if isinstance(member, List) else None
        for member in res
    ]


def format_geo_search_response(
    res: List[List[Union[str, List[str]]]],
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

    for member in res:
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


def format_time(res: List[str], _) -> Tuple[int, int]:
    return int(res[0]), int(res[1])


def format_sorted_set_response(res: List[str], _) -> List[Tuple[str, float]]:
    """
    Format the raw output given by Sorted Set commands, usually the ones that return the member-score
    pairs of Sorted Sets.
    """
    it = iter(res)
    return list(zip(it, map(float, it)))


def to_optional_float_list(res: List[Optional[str]], _) -> List[Optional[float]]:
    """
    Format a list of strings representing floats or None values.
    """

    return [float(value) if value is not None else None for value in res]


def format_set(res, command):
    options = command[3:]

    if "GET" in options:
        return res
    return res == "OK"


def string_to_json(res, _):
    if res is None:
        return None

    return loads(res)


def to_json_list(res, command):
    return [string_to_json(value, command) for value in res]


def ok_to_bool(res, _):
    return res == "OK"


def to_bool(res, _):
    return bool(res)


def to_bool_list(res, _):
    return list(map(bool, res))


def to_optional_bool_list(res, _):
    return [bool(value) if value is not None else None for value in res]


def to_optional_float(res, _):
    if res is None:
        return None

    return float(res)


def to_float(res, _):
    return float(res)


def format_scan(res, _):
    return int(res[0]), res[1]


def format_hscan(res, _):
    return int(res[0]), to_dict(res[1], None)


def format_zscan(res, _):
    return int(res[0]), format_sorted_set_response(res[1], None)


def format_zscore(res, _):
    return float(res) if res is not None else res


def format_search(res, command):
    withdist = "WITHDIST" in command
    withhash = "WITHHASH" in command
    withcoord = "WITHCOORD" in command
    if withdist or withhash or withcoord:
        return format_geo_search_response(res, withdist, withhash, withcoord)

    return res


def format_hrandfield(res, command):
    with_values = "WITHVALUES" in command
    if with_values:
        return to_dict(res, command)

    return res


def format_zadd(res, command):
    incr = "INCR" in command
    if incr:
        return float(res) if res is not None else res

    return res


def format_sorted_set_response_with_score(res, command):
    with_scores = "WITHSCORES" in command
    if with_scores:
        return format_sorted_set_response(res, command)

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
    "GEODIST": to_optional_float,
    "GEOPOS": format_geopos,
    "GEORADIUS": format_search,
    "GEORADIUS_RO": format_search,
    "GEORADIUSBYMEMBER": format_search,
    "GEORADIUSBYMEMBER_RO": format_search,
    "GEOSEARCH": format_search,
    "HEXISTS": to_bool,
    "HGETALL": to_dict,
    "HINCRBYFLOAT": to_float,
    "HRANDFIELD": format_hrandfield,
    "HSCAN": format_hscan,
    "HSETNX": to_bool,
    "JSON.ARRPOP": to_json_list,
    "JSON.GET": string_to_json,
    "JSON.MERGE": ok_to_bool,
    "JSON.MGET": to_json_list,
    "JSON.MSET": ok_to_bool,
    "JSON.NUMINCRBY": string_to_json,
    "JSON.NUMMULTBY": string_to_json,
    "JSON.SET": ok_to_bool,
    "JSON.TOGGLE": to_optional_bool_list,
    "PFADD": to_bool,
    "PFMERGE": ok_to_bool,
    "TIME": format_time,
    "SISMEMBER": to_bool,
    "SMISMEMBER": to_bool_list,
    "SMOVE": to_bool,
    "SSCAN": format_scan,
    "ZADD": format_zadd,
    "ZDIFF": format_sorted_set_response_with_score,
    "ZINCRBY": to_float,
    "ZINTER": format_sorted_set_response_with_score,
    "ZMSCORE": to_optional_float_list,
    "ZPOPMAX": format_sorted_set_response,
    "ZPOPMIN": format_sorted_set_response,
    "ZRANDMEMBER": format_sorted_set_response_with_score,
    "ZRANGE": format_sorted_set_response_with_score,
    "ZRANGEBYSCORE": format_sorted_set_response_with_score,
    "ZREVRANGE": format_sorted_set_response_with_score,
    "ZREVRANGEBYSCORE": format_sorted_set_response_with_score,
    "ZSCAN": format_zscan,
    "ZSCORE": format_zscore,
    "ZUNION": format_sorted_set_response_with_score,
    "INCRBYFLOAT": to_float,
    "PUBSUB NUMSUB": to_dict,
    "FLUSHALL": ok_to_bool,
    "FLUSHDB": ok_to_bool,
    "PSETEX": ok_to_bool,
    "SET": format_set,
    "SETEX": ok_to_bool,
    "SETNX": to_bool,
    "MSET": ok_to_bool,
    "MSETNX": to_bool,
    "HMSET": ok_to_bool,
    "LSET": ok_to_bool,
    "SCRIPT FLUSH": ok_to_bool,
    "SCRIPT EXISTS": to_bool_list,
}


def cast_response(command: List[str], response: RESTResultT):
    """
    Given a command and its response, casts the response using the `FORMATTERS`
    map

    :param command: Used to determine the formatting to apply
    :param response: Response to format
    """

    # get main command
    main_command = command[0]
    if len(command) > 1 and main_command == "SCRIPT":
        main_command = f"{main_command} {command[1]}"

    # format response
    if main_command in FORMATTERS:
        return FORMATTERS[main_command](response, command)

    return response
