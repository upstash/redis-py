from upstash_redis.typing import CommandsProtocol, ResponseType

from upstash_redis.utils.exception import (
    handle_geosearch_exceptions,
    handle_non_deprecated_zrange_exceptions,
    handle_zrangebylex_exceptions,
    handle_georadius_write_exceptions,
)
from upstash_redis.utils.comparison import number_are_not_none
from upstash_redis.schema.commands.parameters import (
    BitFieldOffset,
    GeoMember,
    FloatMinMax,
)

from typing import Any, Iterable, Literal, Optional, Union, List, Dict


class Commands(CommandsProtocol):
    def run(self, command):
        ...

    def bitcount(
        self, key: str, start: Union[int, None] = None, end: Union[int, None] = None
    ) -> ResponseType:
        """
        See https://redis.io/commands/bitcount
        """

        if number_are_not_none(start, end, number=1):
            raise Exception('Both "start" and "end" must be specified.')

        command: List = ["BITCOUNT", key]

        if start is not None:
            command.extend([start, end])

        return self.run(command)

    def bitfield(self, key: str) -> "BitFieldCommands":
        """
        See https://redis.io/commands/bitfield
        """

        return BitFieldCommands(key=key, client=self)

    def bitfield_ro(self, key: str) -> "BitFieldRO":
        """
        See https://redis.io/commands/bitfield_ro
        """

        return BitFieldRO(key=key, client=self)

    def bitop(
        self, operation: Literal["AND", "OR", "XOR", "NOT"], destkey: str, *srckeys: str
    ) -> ResponseType:
        """
        See https://redis.io/commands/bitop
        """

        if len(srckeys) == 0:
            raise Exception("At least one source key must be specified.")

        if operation == "NOT" and len(srckeys) > 1:
            raise Exception(
                'The "NOT " operation takes only one source key as argument.'
            )

        command: List = ["BITOP", operation, destkey, *srckeys]

        return self.run(command)

    def bitpos(
        self,
        key: str,
        bit: Literal[0, 1],
        start: Union[int, None] = None,
        end: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/bitpos
        """

        if start is None and end is not None:
            raise Exception('"end" is specified, but "start" is missing.')

        command: List = ["BITPOS", key, bit]

        if start is not None:
            command.append(start)

        if end is not None:
            command.append(end)

        return self.run(command)

    def getbit(self, key: str, offset: int) -> ResponseType:
        """
        See https://redis.io/commands/getbit
        """

        command: List = ["GETBIT", key, offset]

        return self.run(command)

    def setbit(self, key: str, offset: int, value: Literal[0, 1]) -> ResponseType:
        """
        See https://redis.io/commands/setbit
        """

        command: List = ["SETBIT", key, offset, value]

        return self.run(command)

    def ping(self, message: Union[str, None] = None) -> ResponseType:
        """
        See https://redis.io/commands/ping
        """

        command: List = ["PING"]

        if message is not None:
            command.append(message)

        return self.run(command)

    def echo(self, message: str) -> ResponseType:
        """
        See https://redis.io/commands/echo
        """

        command: List = ["ECHO", message]

        return self.run(command)

    def copy(
        self, source: str, destination: str, replace: bool = False
    ) -> ResponseType:
        """
        See https://redis.io/commands/copy

        :return: A bool if "format_return" is True.
        """

        command: List = ["COPY", source, destination]

        if replace:
            command.append("REPLACE")

        return self.run(command)

    def delete(self, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/del
        """

        if len(keys) == 0:
            raise Exception("At least one key must be deleted.")

        command: List = ["DEL", *keys]

        return self.run(command)

    def exists(self, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/exists
        """

        if len(keys) == 0:
            raise Exception("At least one key must be checked.")

        command: List = ["EXISTS", *keys]

        return self.run(command)

    def expire(self, key: str, seconds: int) -> ResponseType:
        """
        See https://redis.io/commands/expire

        :return: A bool if "format_return" is True.
        """

        command: List = ["EXPIRE", key, seconds]

        return self.run(command)

    def expireat(self, key: str, unix_time_seconds: int) -> ResponseType:
        """
        See https://redis.io/commands/expireat

        :return: A bool if "format_return" is True.
        """

        command: List = ["EXPIREAT", key, unix_time_seconds]

        return self.run(command)

    def keys(self, pattern: str) -> ResponseType:
        """
        See https://redis.io/commands/keys
        """

        command: List = ["KEYS", pattern]

        return self.run(command)

    def persist(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/persist

        :return: A bool if "format_return" is True.
        """

        command: List = ["PERSIST", key]

        return self.run(command)

    def pexpire(self, key: str, milliseconds: int) -> ResponseType:
        """
        See https://redis.io/commands/pexpire

        :return: A bool if "format_return" is True.
        """

        command: List = ["PEXPIRE", key, milliseconds]

        return self.run(command)

    def pexpireat(self, key: str, unix_time_milliseconds: int) -> ResponseType:
        """
        See https://redis.io/commands/pexpireat

        :return: A bool if "format_return" is True.
        """

        command: List = ["PEXPIREAT", key, unix_time_milliseconds]

        return self.run(command)

    def pttl(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/pttl
        """

        command: List = ["PTTL", key]

        return self.run(command)

    def randomkey(self) -> ResponseType:
        """
        See https://redis.io/commands/randomkey
        """

        command: List = ["RANDOMKEY"]

        return self.run(command)

    def rename(self, key: str, newkey: str) -> ResponseType:
        """
        See https://redis.io/commands/rename
        """

        command: List = ["RENAME", key, newkey]

        return self.run(command)

    def renamenx(self, key: str, newkey: str) -> ResponseType:
        """
        See https://redis.io/commands/renamenx

        :return: A bool if "format_return" is True.
        """

        command: List = ["RENAMENX", key, newkey]

        return self.run(command)

    def scan(
        self,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
        scan_type: Union[str, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/scan

        :param scan_type: replacement for "TYPE"
        :param match_pattern: replacement for "MATCH"

        :return: The cursor will be an integer if "format_return" is True.
        """

        command: List = ["SCAN", cursor]

        if match_pattern is not None:
            command.extend(["MATCH", match_pattern])

        if count is not None:
            command.extend(["COUNT", count])

        if scan_type is not None:
            command.extend(["TYPE", scan_type])

        # The raw result is composed of the new cursor and the List of elements.
        return self.run(command)

    def touch(self, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/touch
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["TOUCH", *keys]

        return self.run(command)

    def ttl(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/ttl
        """

        command: List = ["TTL", key]

        return self.run(command)

    def type(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/type
        """

        command: List = ["TYPE", key]

        return self.run(command)

    def unlink(self, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/unlink
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["UNLINK", *keys]

        return self.run(command)

    def geoadd(
        self,
        key: str,
        *members: GeoMember,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> ResponseType:
        """
        See https://redis.io/commands/geoadd

        :param members: a sequence of GeoMember Dict types (longitude, latitude, name).
        """

        if len(members) == 0:
            raise Exception("At least one member must be added.")

        if nx and xx:
            raise Exception('"nx" and "xx" are mutually exclusive.')

        command: List = ["GEOADD", key]

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        if ch:
            command.append("CH")

        for member in members:
            command.extend([member["longitude"], member["latitude"], member["member"]])

        return self.run(command)

    def geodist(
        self,
        key: str,
        member1: str,
        member2: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"] = "M",
    ) -> ResponseType:
        """
        See https://redis.io/commands/geodist

        :return: A float value if "format_return" is True.
        """

        command: List = ["GEODIST", key, member1, member2, unit]

        return self.run(command)

    def geohash(self, key: str, *members: str) -> ResponseType:
        """
        See https://redis.io/commands/geohash
        """

        command: List = ["GEOHASH", key, *members]

        return self.run(command)

    def geopos(self, key: str, *members: str) -> ResponseType:
        """
        See https://redis.io/commands/geopos

        :return: A List of Dicts with either None or the longitude and latitude if "format_return" is True.
        """

        command: List = ["GEOPOS", key, *members]

        return self.run(command)

    def georadius(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        count_any: bool = False,
        sort: Union[Literal["ASC", "DESC"], None] = None,
        store: Union[str, None] = None,
        storedist: Union[str, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/georadius

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True and any of the `with` parameters is used.
        """

        handle_georadius_write_exceptions(
            withdist, withhash, withcoord, count, count_any, store, storedist
        )

        command: List = ["GEORADIUS", key, longitude, latitude, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        if store:
            command.extend(["STORE", store])

        if storedist:
            command.extend(["STOREDIST", storedist])

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.run(command)

    def georadius_ro(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        count_any: bool = False,
        sort: Union[Literal["ASC", "DESC"], None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/georadius_ro

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True and any of the `with` parameters is used.
        """

        if count_any and count is None:
            raise Exception('"count_any" can only be used together with "count".')

        command: List = ["GEORADIUS_RO", key, longitude, latitude, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.run(command)

    def georadiusbymember(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        count_any: bool = False,
        sort: Union[Literal["ASC", "DESC"], None] = None,
        store: Union[str, None] = None,
        storedist: Union[str, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/georadiusbymember

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True and any of the `with` parameters is used.
        """

        handle_georadius_write_exceptions(
            withdist, withhash, withcoord, count, count_any, store, storedist
        )

        command: List = ["GEORADIUSBYMEMBER", key, member, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        if store:
            command.extend(["STORE", store])

        if storedist:
            command.extend(["STOREDIST", storedist])

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.run(command)

    def georadiusbymember_ro(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        count_any: bool = False,
        sort: Union[Literal["ASC", "DESC"], None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/georadiusbymember_ro

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True and any of the `with` parameters is used.
        """

        if count_any and count is None:
            raise Exception('"count_any" can only be used together with "count".')

        command: List = ["GEORADIUSBYMEMBER_RO", key, member, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.run(command)

    def geosearch(
        self,
        key: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        frommember: Union[str, None] = None,
        fromlonlat_longitude: Union[float, None] = None,
        fromlonlat_latitude: Union[float, None] = None,
        byradius: Union[float, None] = None,
        bybox_width: Union[float, None] = None,
        bybox_height: Union[float, None] = None,
        sort: Union[Literal["ASC", "DESC"], None] = None,
        count: Union[int, None] = None,
        count_any: bool = False,
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
    ) -> ResponseType:
        """
        See https://redis.io/commands/geosearch

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True and any of the `with` parameters is used.
        """

        handle_geosearch_exceptions(
            frommember,
            fromlonlat_longitude,
            fromlonlat_latitude,
            byradius,
            bybox_width,
            bybox_height,
            count,
            count_any,
        )

        command: List = ["GEOSEARCH", key]

        if frommember is not None:
            command.extend(["FROMMEMBER", frommember])

        if fromlonlat_longitude is not None:
            command.extend(["FROMLONLAT", fromlonlat_longitude, fromlonlat_latitude])

        if byradius is not None:
            command.extend(["BYRADIUS", byradius])

        if bybox_width is not None:
            command.extend(["BYBOX", bybox_width, bybox_height])

        command.append(unit)

        if sort:
            command.append(sort)

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.run(command)

    def geosearchstore(
        self,
        destination: str,
        source: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        frommember: Union[str, None] = None,
        fromlonlat_longitude: Union[float, None] = None,
        fromlonlat_latitude: Union[float, None] = None,
        byradius: Union[float, None] = None,
        bybox_width: Union[float, None] = None,
        bybox_height: Union[float, None] = None,
        sort: Union[Literal["ASC", "DESC"], None] = None,
        count: Union[int, None] = None,
        count_any: bool = False,
        storedist: bool = False,
    ) -> ResponseType:
        """
        See https://redis.io/commands/geosearchstore

        :param count_any: replacement for "ANY"
        """

        handle_geosearch_exceptions(
            frommember,
            fromlonlat_longitude,
            fromlonlat_latitude,
            byradius,
            bybox_width,
            bybox_height,
            count,
            count_any,
        )

        command: List = ["GEOSEARCHSTORE", destination, source]

        if frommember is not None:
            command.extend(["FROMMEMBER", frommember])

        if fromlonlat_longitude is not None:
            command.extend(["FROMLONLAT", fromlonlat_longitude, fromlonlat_latitude])

        if byradius is not None:
            command.extend(["BYRADIUS", byradius])

        if bybox_width is not None:
            command.extend(["BYBOX", bybox_width, bybox_height])

        command.append(unit)

        if sort:
            command.append(sort)

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if storedist:
            command.append("STOREDIST")

        return self.run(command)

    def hdel(self, key: str, *fields: str) -> ResponseType:
        """
        See https://redis.io/commands/hdel
        """

        if len(fields) == 0:
            raise Exception("At least one field must be deleted.")

        command: List = ["HDEL", key, *fields]

        return self.run(command)

    def hexists(self, key: str, field: str) -> ResponseType:
        """
        See https://redis.io/commands/hexists

        :return: A bool if "format_return" is True.
        """

        command: List = ["HEXISTS", key, field]

        return self.run(command)

    def hget(self, key: str, field: str) -> ResponseType:
        """
        See https://redis.io/commands/hget
        """

        command: List = ["HGET", key, field]

        return self.run(command)

    def hgetall(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/hgetall

        :return: A Dict of field-value pairs if "format_return" is True.
        """

        command: List = ["HGETALL", key]

        return self.run(command)

    def hincrby(self, key: str, field: str, increment: int) -> ResponseType:
        """
        See https://redis.io/commands/hincrby
        """

        command: List = ["HINCRBY", key, field, increment]

        return self.run(command)

    def hincrbyfloat(self, key: str, field: str, increment: float) -> ResponseType:
        """
        See https://redis.io/commands/hincrbyfloat

        :return: A float if "format_return" is True.
        """

        command: List = ["HINCRBYFLOAT", key, field, increment]

        return self.run(command)

    def hkeys(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/hkeys
        """

        command: List = ["HKEYS", key]

        return self.run(command)

    def hlen(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/hlen
        """

        command: List = ["HLEN", key]

        return self.run(command)

    def hmget(self, key: str, *fields: str) -> ResponseType:
        """
        See https://redis.io/commands/hmget
        """

        if len(fields) == 0:
            raise Exception("At least one field must be specified.")

        command: List = ["HMGET", key, *fields]

        return self.run(command)

    def hmset(self, key: str, field_value_pairs: Dict) -> ResponseType:
        """
        See https://redis.io/commands/hmset
        """

        command: List = ["HMSET", key]

        for field, value in field_value_pairs.items():
            command.extend([field, value])

        return self.run(command)

    def hrandfield(
        self, key: str, count: Union[int, None] = None, withvalues: bool = False
    ) -> ResponseType:
        """
        See https://redis.io/commands/hrandfield

        :return: A Dict of field-value pairs if "count" and "withvalues" are specified and "format_return" is True.
        """

        if count is None and withvalues:
            raise Exception('"withvalues" can only be used together with "count"')

        command: List = ["HRANDFIELD", key]

        if count is not None:
            command.extend([count])

            if withvalues:
                command.append("WITHVALUES")

        return self.run(command)

    def hscan(
        self,
        name: str,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/hscan

        :param match_pattern: replacement for "MATCH"
        """

        command: List = ["HSCAN", name, cursor]

        if match_pattern is not None:
            command.extend(["MATCH", match_pattern])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        return self.run(command)

    def hset(
        self,
        name: str,
        key: Optional[str] = None,
        val: Optional[str] = None,
        field_value_pairs: Optional[Dict] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/hset
        """
        command: List = ["HSET", name]

        if key is None and field_value_pairs is None:
            raise Exception("'hset' with no key value pairs")

        if key and val:
            command.extend([key, val])

        if field_value_pairs is not None:
            for field, value in field_value_pairs.items():
                command.extend([field, value])

        return self.run(command)

    def hsetnx(self, key: str, field: str, value: Any) -> ResponseType:
        """
        See https://redis.io/commands/hsetnx

        :return: A bool if "format_return" is True.
        """

        command: List = ["HSETNX", key, field, value]

        return self.run(command)

    def hstrlen(self, key: str, field: str) -> ResponseType:
        """
        See https://redis.io/commands/hstrlen
        """

        command: List = ["HSTRLEN", key, field]

        return self.run(command)

    def hvals(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/hvals
        """

        command: List = ["HVALS", key]

        return self.run(command)

    def pfadd(self, key: str, *elements: Any) -> ResponseType:
        """
        See https://redis.io/commands/pfadd

        :return: A bool if "format_return" is True.
        """

        command: List = ["PFADD", key, *elements]

        return self.run(command)

    def pfcount(self, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/pfcount
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["PFCOUNT", *keys]

        return self.run(command)

    def pfmerge(self, destkey: str, *sourcekeys: str) -> ResponseType:
        """
        See https://redis.io/commands/pfmerge
        """

        command: List = ["PFMERGE", destkey, *sourcekeys]

        return self.run(command)

    def lindex(self, key: str, index: int) -> ResponseType:
        """
        See https://redis.io/commands/lindex
        """

        command: List = ["LINDEX", key, index]

        return self.run(command)

    def linsert(
        self,
        key: str,
        position: Literal["BEFORE", "AFTER", "before", "after"],
        pivot: Any,
        element: Any,
    ) -> ResponseType:
        """
        See https://redis.io/commands/linsert
        """

        command: List = ["LINSERT", key, position, pivot, element]

        return self.run(command)

    def llen(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/llen
        """

        command: List = ["LLEN", key]

        return self.run(command)

    def lmove(
        self,
        source: str,
        destination: str,
        source_position: Literal["LEFT", "RIGHT"] = "LEFT",
        destination_position: Literal["LEFT", "RIGHT"] = "RIGHT",
    ) -> ResponseType:
        """
        See https://redis.io/commands/lmove
        """

        command: List = [
            "LMOVE",
            source,
            destination,
            source_position,
            destination_position,
        ]

        return self.run(command)

    def lpop(self, key: str, count: Union[int, None] = None) -> ResponseType:
        """
        See https://redis.io/commands/lpop

        :param count: defaults to 1 on the server side
        """

        command: List = ["LPOP", key]

        if count is not None:
            command.append(count)

        return self.run(command)

    def lpos(
        self,
        key: str,
        element: Any,
        rank: Union[int, None] = None,
        count: Union[int, None] = None,
        maxlen: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/lpos
        """

        command: List = ["LPOS", key, element]

        if rank is not None:
            command.extend(["RANK", rank])

        if count is not None:
            command.extend(["COUNT", count])

        if maxlen is not None:
            command.extend(["MAXLEN", maxlen])

        return self.run(command)

    def lpush(self, key: str, *elements: Any) -> ResponseType:
        """
        See https://redis.io/commands/lpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["LPUSH", key, *elements]

        return self.run(command)

    def lpushx(self, key: str, *elements: Any) -> ResponseType:
        """
        See https://redis.io/commands/lpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["LPUSHX", key, *elements]

        return self.run(command)

    def lrange(self, key: str, start: int, stop: int) -> ResponseType:
        """
        See https://redis.io/commands/lrange
        """

        command: List = ["LRANGE", key, start, stop]

        return self.run(command)

    def lrem(self, key: str, count: int, element: Any) -> ResponseType:
        """
        See https://redis.io/commands/lrem
        """

        command: List = ["LREM", key, count, element]

        return self.run(command)

    def lset(self, key: str, index: int, element: Any) -> ResponseType:
        """
        See https://redis.io/commands/lset
        """

        command: List = ["LSET", key, index, element]

        return self.run(command)

    def ltrim(self, key: str, start: int, stop: int) -> ResponseType:
        """
        See https://redis.io/commands/ltrim
        """

        command: List = ["LTRIM", key, start, stop]

        return self.run(command)

    def rpop(self, key: str, count: Union[int, None] = None) -> ResponseType:
        """
        See https://redis.io/commands/rpop

        :param count: defaults to 1 on the server side
        """

        command: List = ["RPOP", key]

        if count is not None:
            command.append(count)

        return self.run(command)

    def rpoplpush(self, source: str, destination: str) -> ResponseType:
        """
        See https://redis.io/commands/rpoplpush
        """

        command: List = ["RPOPLPUSH", source, destination]

        return self.run(command)

    def rpush(self, key: str, *elements: Any) -> ResponseType:
        """
        See https://redis.io/commands/rpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["RPUSH", key, *elements]

        return self.run(command)

    def rpushx(self, key: str, *elements: Any) -> ResponseType:
        """
        See https://redis.io/commands/rpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["RPUSHX", key, *elements]

        return self.run(command)

    def publish(self, channel: str, message: str) -> ResponseType:
        """
        See https://redis.io/commands/publish
        """

        command: List = ["PUBLISH", channel, message]

        return self.run(command)

    def eval(
        self,
        script: str,
        keys: Union[List[str], None] = None,
        args: Union[List, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/eval

        The number of keys is calculated automatically.
        """

        command: List = ["EVAL", script]

        if keys is not None:
            command.extend([len(keys), *keys])
        else:
            command.append(0)

        if args:
            command.extend(args)

        return self.run(command)

    def evalsha(
        self,
        sha1: str,
        keys: Union[List[str], None] = None,
        args: Union[List, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/evalsha

        The number of keys is calculated automatically.
        """

        command: List = ["EVALSHA", sha1]

        if keys is not None:
            command.extend([len(keys), *keys])
        else:
            command.append(0)

        if args:
            command.extend(args)

        return self.run(command)

    def dbsize(self) -> ResponseType:
        """
        See https://redis.io/commands/dbsize
        """

        command: List = ["DBSIZE"]

        return self.run(command)

    def flushall(
        self, mode: Union[Literal["ASYNC", "SYNC"], None] = None
    ) -> ResponseType:
        """
        See https://redis.io/commands/flushall
        """

        command: List = ["FLUSHALL"]

        if mode:
            command.append(mode)

        return self.run(command)

    def flushdb(
        self, mode: Union[Literal["ASYNC", "SYNC"], None] = None
    ) -> ResponseType:
        """
        See https://redis.io/commands/flushdb
        """

        command: List = ["FLUSHDB"]

        if mode:
            command.append(mode)

        return self.run(command)

    def time(self) -> ResponseType:
        """
        See https://redis.io/commands/time

        :return: A Dict with the keys "seconds" and "microseconds" if self.format_return is True.
        """

        command: List = ["TIME"]

        return self.run(command)

    def sadd(self, key: str, *members: Any) -> ResponseType:
        """
        See https://redis.io/commands/sadd
        """

        if len(members) == 0:
            raise Exception("At least one member must be added.")

        command: List = ["SADD", key, *members]

        return self.run(command)

    def scard(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/scard
        """

        command: List = ["SCARD", key]

        return self.run(command)

    def sdiff(self, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/sdiff
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SDIFF", *keys]

        return self.run(command)

    def sdiffstore(self, destination: str, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/sdiffstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SDIFFSTORE", destination, *keys]

        return self.run(command)

    def sinter(self, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/sinter
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SINTER", *keys]

        return self.run(command)

    def sinterstore(self, destination: str, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/sinterstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SINTERSTORE", destination, *keys]

        return self.run(command)

    def sismember(self, key: str, member: Any) -> ResponseType:
        """
        See https://redis.io/commands/sismember

        :return: A bool if self.format_return is True.
        """

        command: List = ["SISMEMBER", key, member]

        return self.run(command)

    def smembers(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/smembers
        """

        command: List = ["SMEMBERS", key]

        return self.run(command)

    def smismember(self, key: str, *members: Any) -> ResponseType:
        """
        See https://redis.io/commands/smismember

        :return: A bool list if self.format_return is True.
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["SMISMEMBER", key, *members]

        return self.run(command)

    def smove(self, source: str, destination: str, member: Any) -> ResponseType:
        """
        See https://redis.io/commands/smove

        :return: A bool if self.format_return is True.
        """

        command: List = ["SMOVE", source, destination, member]

        return self.run(command)

    def spop(self, key: str, count: Union[int, None] = None) -> ResponseType:
        """
        See https://redis.io/commands/spop

        :param count: defaults to 1 on the server side
        """

        command: List = ["SPOP", key]

        if count is not None:
            command.append(count)

        return self.run(command)

    def srandmember(self, key: str, count: Union[int, None] = None) -> ResponseType:
        """
        See https://redis.io/commands/srandmember

        :param count: defaults to 1 on the server side
        """

        command: List = ["SRANDMEMBER", key]

        if count is not None:
            command.append(count)

        return self.run(command)

    def srem(self, key: str, *members: Any) -> ResponseType:
        """
        See https://redis.io/commands/srem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["SREM", key, *members]

        return self.run(command)

    def sscan(
        self,
        key: str,
        cursor: int = 0,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/sscan

        :param match_pattern: replacement for "MATCH"

        :return: The cursor will be an integer if "format_return" is True.
        """

        command: List = ["SSCAN", key, cursor]

        if match_pattern is not None:
            command.extend(["MATCH", match_pattern])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        return self.run(command)

    def sunion(self, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/sunion
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SUNION", *keys]

        return self.run(command)

    def sunionstore(self, destination: str, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/sunionstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SUNIONSTORE", destination, *keys]

        return self.run(command)

    def zadd(
        self,
        key: str,
        score_member_pairs: Dict,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
        incr: bool = False,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zadd

        :param score_member_pairs: a Dict containing members and their scores.

        :return: A float representing the number of elements added or None if "incr" is False
        and "format_return" is True.
        """

        if nx and xx:
            raise Exception('"nx" and "xx" are mutually exclusive.')

        if gt and lt:
            raise Exception('"gt" and "lt" are mutually exclusive.')

        if nx and (gt or lt):
            raise Exception('"nx" and "gt" or "lt" are mutually exclusive.')

        command: List = ["ZADD", key]

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        if gt:
            command.append("GT")

        if lt:
            command.append("LT")

        if ch:
            command.append("CH")

        if incr:
            command.append("INCR")

        for name, score in score_member_pairs.items():
            command.extend([score, name])

        return self.run(command)

    def zcard(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/zcard
        """

        command: List = ["ZCARD", key]

        return self.run(command)

    def zcount(
        self, key: str, min_score: FloatMinMax, max_score: FloatMinMax
    ) -> ResponseType:
        """
        See https://redis.io/commands/zcount

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: List = ["ZCOUNT", key, min_score, max_score]

        return self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be List[str].
    """

    def zdiff(self, keys: List[str], withscores: bool = False) -> ResponseType:
        """
        See https://redis.io/commands/zdiff

        The number of keys is calculated automatically.

        :return: A Dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZDIFF", len(keys), *keys]

        if withscores:
            command.append("WITHSCORES")

        return self.run(command)

    def zdiffstore(self, destination: str, keys: List[str]) -> ResponseType:
        """
        See https://redis.io/commands/zdiffstore

        The number of keys is calculated automatically.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZDIFFSTORE", destination, len(keys), *keys]

        return self.run(command)

    def zincrby(self, key: str, increment: float, member: str) -> ResponseType:
        """
        See https://redis.io/commands/zincrby

        :return: A float if "format_return" is True.
        """

        command: List = ["ZINCRBY", key, increment, member]

        return self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be List[str].
    """

    def zinter(
        self,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
        withscores: bool = False,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zinter

        The number of keys is calculated automatically.

        :return: A Dict of member-score pairs if "withscores" and "format_return" are True.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZINTER", len(keys), *keys]

        if weights:
            command.extend(["WEIGHTS", *weights])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        if withscores:
            command.append("WITHSCORES")

        return self.run(command)

    def zinterstore(
        self,
        destination: str,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zinterstore

        The number of keys is calculated automatically.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZINTERSTORE", destination, len(keys), *keys]

        if weights:
            command.extend(["WEIGHTS", *weights])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        return self.run(command)

    def zlexcount(self, key: str, min_score: str, max_score: str) -> ResponseType:
        """
        See https://redis.io/commands/zlexcount

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"
        """

        if not min_score.startswith(("(", "[", "+", "-")) or not max_score.startswith(
            ("(", "[", "+", "-")
        ):
            raise Exception(
                "\"min_score\" and \"max_score\" must either start with '(' or '[' or be '+' or '-'."
            )

        command: List = ["ZLEXCOUNT", key, min_score, max_score]

        return self.run(command)

    def zmscore(self, key: str, members: List[str]) -> ResponseType:
        """
        See https://redis.io/commands/zmscore

        :return: A List of float or None values if "format_return" is True.
        """

        if len(members) == 0:
            raise Exception("At least one member must be specified.")

        command: List = ["ZMSCORE", key, *members]

        return self.run(command)

    def zpopmax(self, key: str, count: Union[int, None] = None) -> ResponseType:
        """
        See https://redis.io/commands/zpopmax

        :param count: defaults to 1 on the server side

        :return: A Dict of member-score pairs if "format_return" is True.
        """

        command: List = ["ZPOPMAX", key]

        if count is not None:
            command.append(count)

        return self.run(command)

    def zpopmin(self, key: str, count: Union[int, None] = None) -> ResponseType:
        """
        See https://redis.io/commands/zpopmin

        :param count: defaults to 1 on the server side

        :return: A Dict of member-score pairs if "format_return" is True.
        """

        command: List = ["ZPOPMIN", key]

        if count is not None:
            command.append(count)

        return self.run(command)

    def zrandmember(
        self, key: str, count: Union[int, None] = None, withscores: bool = False
    ) -> ResponseType:
        """
        See https://redis.io/commands/zrandmember

        :param count: defaults to 1 on the server side

        :return: A Dict of member-score pairs if "withscores" and "format_return" are True.
        """

        if count is None and withscores:
            raise Exception('"withscores" can only be used with "count".')

        command: List = ["ZRANDMEMBER", key]

        if count is not None:
            command.append(count)

            if withscores:
                command.append("WITHSCORES")

        return self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be List[str].
    """

    def zrange(
        self,
        key: str,
        start: FloatMinMax,
        stop: FloatMinMax,
        range_method: Union[Literal["BYSCORE", "BYLEX"], None] = None,
        rev: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
        withscores: bool = False,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zrange

        If you need to use "-inf" and "+inf", please write them as strings.

        :return: A Dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        handle_non_deprecated_zrange_exceptions(
            range_method, start, stop, limit_offset, limit_count
        )

        command: List = ["ZRANGE", key, start, stop]

        if range_method:
            command.append(range_method)

        if rev:
            command.append("REV")

        if limit_offset is not None:
            command.extend(["LIMIT", limit_offset, limit_count])

        if withscores:
            command.append("WITHSCORES")

        return self.run(command)

    def zrangebylex(
        self,
        key: str,
        min_score: str,
        max_score: str,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zrangebylex

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"
        """

        handle_zrangebylex_exceptions(min_score, max_score, limit_offset, limit_count)

        command: List = ["ZRANGEBYLEX", key, min_score, max_score]

        if limit_offset is not None:
            command.extend(["LIMIT", limit_offset, limit_count])

        return self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "withscores" is True or not, its raw return type will be List[str].
    """

    def zrangebyscore(
        self,
        key: str,
        min_score: FloatMinMax,
        max_score: FloatMinMax,
        withscores: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zrangebyscore

        If you need to use "-inf" and "+inf", please write them as strings.

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"

        :return: A Dict of member-score pairs if "withscores" and "format_return" are True.
        """

        if number_are_not_none(limit_offset, limit_count, number=1):
            raise Exception('Both "offset" and "count" must be specified.')

        command: List = ["ZRANGEBYSCORE", key, min_score, max_score]

        if limit_offset is not None:
            command.extend(["LIMIT", limit_offset, limit_count])

        if withscores:
            command.append("WITHSCORES")

        return self.run(command)

    def zrangestore(
        self,
        dst: str,
        src: str,
        start: FloatMinMax,
        stop: FloatMinMax,
        range_method: Union[Literal["BYSCORE", "BYLEX"], None] = None,
        rev: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zrangestore

        :param start: replacement for "MIN"
        :param stop: replacement for "MAX"
        """

        handle_non_deprecated_zrange_exceptions(
            range_method, start, stop, limit_offset, limit_count
        )

        command: List = ["ZRANGESTORE", dst, src, start, stop]

        if range_method:
            command.append(range_method)

        if rev:
            command.append("REV")

        if limit_offset is not None:
            command.extend(["LIMIT", limit_offset, limit_count])

        return self.run(command)

    def zrank(self, key: str, member: str) -> ResponseType:
        """
        See https://redis.io/commands/zrank
        """

        command: List = ["ZRANK", key, member]

        return self.run(command)

    def zrem(self, key: str, *members: str) -> ResponseType:
        """
        See https://redis.io/commands/zrem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["ZREM", key, *members]

        return self.run(command)

    def zremrangebylex(self, key: str, min_score: str, max_score: str) -> ResponseType:
        """
        See https://redis.io/commands/zremrangebylex

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"
        """

        if not min_score.startswith(("(", "[", "+", "-")) or not max_score.startswith(
            ("(", "[", "+", "-")
        ):
            raise Exception(
                "\"min_score\" and \"max_score\" must either start with '(' or '[' or be '+' or '-'."
            )

        command: List = ["ZREMRANGEBYLEX", key, min_score, max_score]

        return self.run(command)

    def zremrangebyrank(self, key: str, start: int, stop: int) -> ResponseType:
        """
        See https://redis.io/commands/zremrangebyrank
        """

        command: List = ["ZREMRANGEBYRANK", key, start, stop]

        return self.run(command)

    def zremrangebyscore(
        self, key: str, min_score: FloatMinMax, max_score: FloatMinMax
    ) -> ResponseType:
        """
        See https://redis.io/commands/zremrangebyscore\
        
        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: List = ["ZREMRANGEBYSCORE", key, min_score, max_score]

        return self.run(command)

    """
    This has actually 3 return scenarios, but,
    whether "with_scores" is True or not, its raw return type will be List[str].
    """

    def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> ResponseType:
        """
        See https://redis.io/commands/zrevrange

        :return: A Dict of member-score pairs if "withscores" and "format_return" are True.
        """

        command: List = ["ZREVRANGE", key, start, stop]

        if withscores:
            command.append("WITHSCORES")

        return self.run(command)

    def zrevrangebylex(
        self,
        key: str,
        max_score: str,
        min_score: str,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zrevrangebylex

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"
        """

        handle_zrangebylex_exceptions(min_score, max_score, limit_offset, limit_count)

        command: List = ["ZREVRANGEBYLEX", key, max_score, min_score]

        if limit_offset is not None:
            command.extend(["LIMIT", limit_offset, limit_count])

        return self.run(command)

    """
    This has actually 3 return scenarios, but,
    whether "withscores" is True or not, its raw return type will be List[str].
    """

    def zrevrangebyscore(
        self,
        key: str,
        max_score: FloatMinMax,
        min_score: FloatMinMax,
        withscores: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zrevrangebyscore

        If you need to use "-inf" and "+inf", please write them as strings.

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"

        :return: A Dict of member-score pairs if "withscores" and "format_return" are True.
        """

        if number_are_not_none(limit_offset, limit_count, number=1):
            raise Exception('Both "limit_offset" and "limit_count" must be specified.')

        command: List = ["ZREVRANGEBYSCORE", key, max_score, min_score]

        if limit_offset is not None:
            command.extend(["LIMIT", limit_offset, limit_count])

        if withscores:
            command.append("WITHSCORES")

        return self.run(command)

    def zrevrank(self, key: str, member: str) -> ResponseType:
        """
        See https://redis.io/commands/zrevrank
        """

        command: List = ["ZREVRANK", key, member]

        return self.run(command)

    def zscan(
        self,
        key: str,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zscan

        :param match_pattern: replacement for "MATCH"
        """

        command: List = ["ZSCAN", key, cursor]

        if match_pattern is not None:
            command.extend(["MATCH", match_pattern])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        return self.run(command)

    def zscore(self, key: str, member: str) -> ResponseType:
        """
        See https://redis.io/commands/zscore

        :return: A float or None if "format_return" is True.
        """

        command: List = ["ZSCORE", key, member]

        return self.run(command)

    """
    This has actually 3 return scenarios, but,
    whether "withscores" is True or not, its raw return type will be List[str].
    """

    def zunion(
        self,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
        withscores: bool = False,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zunion

        The number of keys is calculated automatically.

        :return: A Dict of member-score pairs if "withscores" and "format_return" are True.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZUNION", len(keys), *keys]

        if weights:
            command.extend(["WEIGHTS", *weights])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        if withscores:
            command.append("WITHSCORES")

        return self.run(command)

    def zunionstore(
        self,
        destination: str,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/zunionstore

        The number of keys is calculated automatically.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZUNIONSTORE", destination, len(keys), *keys]

        if weights:
            command.extend(["WEIGHTS", *weights])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        return self.run(command)

    def append(self, key: str, value: Any) -> ResponseType:
        """
        See https://redis.io/commands/append
        """

        command: List = ["APPEND", key, value]

        return self.run(command)

    def decr(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/decr
        """

        command: List = ["DECR", key]

        return self.run(command)

    def decrby(self, key: str, decrement: int) -> ResponseType:
        """
        See https://redis.io/commands/decrby
        """

        command: List = ["DECRBY", key, decrement]

        return self.run(command)

    def get(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/get
        """

        command: List = ["GET", key]

        return self.run(command)

    def getdel(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/getdel
        """

        command: List = ["GETDEL", key]

        return self.run(command)

    def getex(
        self,
        key: str,
        ex: Union[int, None] = None,
        px: Union[int, None] = None,
        exat: Union[int, None] = None,
        pxat: Union[int, None] = None,
        persist: Union[bool, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/getex
        """

        if (ex or px or exat or pxat or persist) and not number_are_not_none(
            ex, px, exat, pxat, persist, number=1
        ):
            raise Exception("Exactly one of the expiration settings must be specified.")

        command: List = ["GETEX", key]

        if ex is not None:
            command.extend(["EX", ex])

        if px is not None:
            command.extend(["PX", px])

        if exat is not None:
            command.extend(["EXAT", exat])

        if pxat is not None:
            command.extend(["PXAT", pxat])

        if persist is not None:
            command.append("PERSIST")

        return self.run(command)

    def getrange(self, key: str, start: int, end: int) -> ResponseType:
        """
        See https://redis.io/commands/getrange
        """

        command: List = ["GETRANGE", key, start, end]

        return self.run(command)

    def getset(self, key: str, value: Any) -> ResponseType:
        """
        See https://redis.io/commands/getset
        """

        command: List = ["GETSET", key, value]

        return self.run(command)

    def incr(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/incr
        """

        command: List = ["INCR", key]

        return self.run(command)

    def incrby(self, key: str, increment: int) -> ResponseType:
        """
        See https://redis.io/commands/incrby
        """

        command: List = ["INCRBY", key, increment]

        return self.run(command)

    def incrbyfloat(self, key: str, increment: float) -> ResponseType:
        """
        See https://redis.io/commands/incrbyfloat

        :return: A float if "format_return" is True.
        """

        command: List = ["INCRBYFLOAT", key, increment]

        return self.run(command)

    def mget(self, *keys: str) -> ResponseType:
        """
        See https://redis.io/commands/mget
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["MGET", *keys]

        return self.run(command)

    def mset(self, key_value_pairs: Dict) -> ResponseType:
        """
        See https://redis.io/commands/mset
        """

        command: List = ["MSET"]

        for key, value in key_value_pairs.items():
            command.extend([key, value])

        return self.run(command)

    def msetnx(self, key_value_pairs: Dict) -> ResponseType:
        """
        See https://redis.io/commands/msetnx
        """

        command: List = ["MSETNX"]

        for key, value in key_value_pairs.items():
            command.extend([key, value])

        return self.run(command)

    def psetex(self, key: str, milliseconds: int, value: Any) -> ResponseType:
        """
        See https://redis.io/commands/psetex
        """

        command: List = ["PSETEX", key, milliseconds, value]

        return self.run(command)

    def set(
        self,
        key: str,
        value: Any,
        nx: Union[bool, None] = None,
        xx: Union[bool, None] = None,
        get: Union[bool, None] = None,
        ex: Union[int, None] = None,
        px: Union[int, None] = None,
        exat: Union[int, None] = None,
        pxat: Union[int, None] = None,
        keepttl: Union[bool, None] = None,
    ) -> ResponseType:
        """
        See https://redis.io/commands/set
        """

        if nx and xx:
            raise Exception('"nx" and "xx" are mutually exclusive.')

        if (ex or px or exat or pxat or keepttl) and not number_are_not_none(
            ex, px, exat, pxat, keepttl, number=1
        ):
            raise Exception("Exactly one of the expiration settings must be specified.")

        if nx and get:
            raise Exception('"nx" and "get" are mutually exclusive.')

        command: List = ["SET", key, value]

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        if get:
            command.append("GET")

        if ex is not None:
            command.extend(["EX", ex])

        if px is not None:
            command.extend(["PX", px])

        if exat is not None:
            command.extend(["EXAT", exat])

        if pxat is not None:
            command.extend(["PXAT", pxat])

        if keepttl:
            command.append("KEEPTTL")

        return self.run(command)

    def setex(self, key: str, seconds: int, value: Any) -> ResponseType:
        """
        See https://redis.io/commands/setex
        """

        command: List = ["SETEX", key, seconds, value]

        return self.run(command)

    def setnx(self, key: str, value: Any) -> ResponseType:
        """
        See https://redis.io/commands/setnx
        """

        command: List = ["SETNX", key, value]

        return self.run(command)

    def setrange(self, key: str, offset: int, value: Any) -> ResponseType:
        """
        See https://redis.io/commands/setrange
        """

        command: List = ["SETRANGE", key, offset, value]

        return self.run(command)

    def strlen(self, key: str) -> ResponseType:
        """
        See https://redis.io/commands/strlen
        """

        command: List = ["STRLEN", key]

        return self.run(command)

    def substr(self, key: str, start: int, end: int) -> ResponseType:
        """
        See https://redis.io/commands/substr
        """

        command: List = ["SUBSTR", key, start, end]

        return self.run(command)

    def script_exists(self, *sha1: str) -> ResponseType:
        """
        See https://redis.io/commands/script-exists

        :return: A List of bools if "format_return" is True.
        """

        if len(sha1) == 0:
            raise Exception("At least one sha1 digests must be provided.")

        command: List = ["SCRIPT", "EXISTS", *sha1]

        return self.run(command)

    def script_flush(
        self, mode: Optional[Literal["ASYNC", "SYNC"]] = None
    ) -> ResponseType:
        """
        See https://redis.io/commands/script-flush
        """

        command: List = ["SCRIPT", "FLUSH"]

        if mode:
            command.append(mode)

        return self.run(command)

    def script_load(self, script: str) -> ResponseType:
        """
        See https://redis.io/commands/script-load
        """

        command: List = ["SCRIPT", "LOAD", script]

        return self.run(command)

    # def pubsub_channels(self, pattern: Union[str, None] = None) -> ResponseType:
    #     """
    #     See https://redis.io/commands/pubsub-channels
    #     """

    #     command: List = ["PUBSUB", "CHANNELS"]

    #     if pattern is not None:
    #         command.append(pattern)

    #     return self.run(command)

    # def pubsub_numpat(self) -> ResponseType:
    #     """
    #     See https://redis.io/commands/pubsub-numpat
    #     """

    #     command: List = ["PUBSUB", "NUMPAT"]

    #     return self.run(command)

    # def pubsub_numsub(
    #     self, *channels: str
    # ) -> ResponseType:
    #     """
    #     See https://redis.io/commands/pubsub-numsub

    #     :return: A Dict with channel-number_of_subscribers pairs if "format_return" is True.
    #     """

    #     command: List = ["PUBSUB", "NUMSUB", *channels]

    #     return self.run(command)


# It doesn't inherit from "Redis" mainly because of the methods signatures.
class BitFieldCommands:
    def __init__(self, client: Commands, key: str):
        self.client = client
        self.command: List = ["BITFIELD", key]

    def get(self, encoding: str, offset: BitFieldOffset) -> "BitFieldCommands":
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    def set(
        self, encoding: str, offset: BitFieldOffset, value: int
    ) -> "BitFieldCommands":
        """
        Set the specified bit field and returns its old value.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["SET", encoding, offset, value]
        self.command.extend(_command)

        return self

    def incrby(
        self, encoding: str, offset: BitFieldOffset, increment: int
    ) -> "BitFieldCommands":
        """
        Increments or decrements (if a negative increment is given) the specified bit field and returns the new value.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["INCRBY", encoding, offset, increment]
        self.command.extend(_command)

        return self

    def overflow(self, overflow: Literal["WRAP", "SAT", "FAIL"]) -> "BitFieldCommands":
        """
        Where an integer encoding is expected, it can be composed by prefixing with i
        for signed integers and u for unsigned integers with the number of bits of our integer encoding.
        So for example u8 is an unsigned integer of 8 bits and i16 is a signed integer of 16 bits.
        The supported encodings are up to 64 bits for signed integers, and up to 63 bits for unsigned integers.
        This limitation with unsigned integers is due to the fact that currently the Redis protocol is unable to
        return 64-bit unsigned integers as replies.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["OVERFLOW", overflow]
        self.command.extend(_command)

        return self

    def execute(self) -> ResponseType:
        return self.client.run(command=self.command)


class BitFieldRO:
    def __init__(self, client: Commands, key: str):
        self.client = client
        self.command: List = ["BITFIELD_RO", key]

    def get(self, encoding: str, offset: BitFieldOffset) -> "BitFieldRO":
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield_ro
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    def execute(self) -> ResponseType:
        return self.client.run(command=self.command)


"""
class ACL:
    def __init__(self, client: Commands):
        self.client = client

    async def cat(self, category: Union[str, None] = None) -> List[str]:
        # See https://redis.io/commands/acl-cat

        command: List = ["ACL", "CAT"]

        if category is not None:
            command.append(category)

        return await self.client.run(command=command)

    async def deluser(self, *usernames: str) -> int:
        # See https://redis.io/commands/acl-deluser
        
        if len(usernames) == 0:
            raise Exception("At least one username must be provided.")

        command: List = ["ACL", "DELUSER", *usernames]

        return await self.client.run(command=command)

    async def genpass(self, bits: Union[int, None] = None) -> str:
        # See https://redis.io/commands/acl-genpass

        command: List = ["ACL", "GENPASS"]

        if bits is not None:
            command.append(bits)

        return await self.client.run(command=command)

    # Is it possible to format this output?
    async def getuser(self, username: str) -> Union[List[str], None]:
        # See https://redis.io/commands/acl-getuser

        command: List = ["ACL", "GETUSER", username]

        return await self.client.run(command=command)

    async def List_rules(self) -> List[str]:
        # See https://redis.io/commands/acl-List

        command = ["ACL", "LIST"]

        return await self.client.run(command=command)

    async def load(self) -> str:
        # See https://redis.io/commands/acl-load

        command = ["ACL", "LOAD"]

        return await self.client.run(command=command)

    async def log(self, count: Union[int, None] = None, reset: bool = False) -> List[str]:
        # See https://redis.io/commands/acl-log

        if count is not None and reset:
            raise Exception("Cannot specify both "count" and "reset".")

        command: List = ["ACL", "LOG"]

        if count is not None:
            command.append(count)

        if reset:
            command.append("RESET")

        return await self.client.run(command=command)

    async def save(self, count: Union[int, None] = None, reset: bool = False) -> List[str]:
        # See https://redis.io/commands/acl-save

        command: List = ["ACL", "SAVE"]

        return await self.client.run(command=command)

    async def setuser():
        ...
    
    async def users():
        ...

    async def whoami():
        ...


"""
