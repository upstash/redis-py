from typing import Any, Awaitable, Dict, List, Literal, Optional, Tuple, Union

from upstash_redis.typing import FloatMinMaxT
from upstash_redis.utils import (
    handle_georadius_write_exceptions,
    handle_geosearch_exceptions,
    handle_non_deprecated_zrange_exceptions,
    handle_zrangebylex_exceptions,
    number_are_not_none,
)

ResponseT = Union[Awaitable, Any]


class Commands:
    def execute(self, command: List) -> ResponseT:
        raise NotImplementedError("execute")

    def bitcount(
        self, key: str, start: Union[int, None] = None, end: Union[int, None] = None
    ) -> ResponseT:
        """
        See https://redis.io/commands/bitcount
        """

        if number_are_not_none(start, end, number=1):
            raise Exception('Both "start" and "end" must be specified.')

        command: List = ["BITCOUNT", key]

        if start is not None:
            command.extend([start, end])

        return self.execute(command)

    def bitfield(self, key: str) -> "BitFieldCommands":
        """
        See https://redis.io/commands/bitfield
        """

        return BitFieldCommands(key=key, client=self)

    def bitfield_ro(self, key: str) -> "BitFieldROCommands":
        """
        See https://redis.io/commands/bitfield_ro
        """

        return BitFieldROCommands(key=key, client=self)

    def bitop(
        self, operation: Literal["AND", "OR", "XOR", "NOT"], destkey: str, *keys: str
    ) -> ResponseT:
        """
        See https://redis.io/commands/bitop
        """

        if len(keys) == 0:
            raise Exception("At least one source key must be specified.")

        if operation == "NOT" and len(keys) > 1:
            raise Exception(
                'The "NOT " operation takes only one source key as argument.'
            )

        command: List = ["BITOP", operation, destkey, *keys]

        return self.execute(command)

    def bitpos(
        self,
        key: str,
        bit: Literal[0, 1],
        start: Union[int, None] = None,
        end: Union[int, None] = None,
    ) -> ResponseT:
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

        return self.execute(command)

    def getbit(self, key: str, offset: int) -> ResponseT:
        """
        See https://redis.io/commands/getbit
        """

        command: List = ["GETBIT", key, offset]

        return self.execute(command)

    def setbit(self, key: str, offset: int, value: Literal[0, 1]) -> ResponseT:
        """
        See https://redis.io/commands/setbit
        """

        command: List = ["SETBIT", key, offset, value]

        return self.execute(command)

    def ping(self, message: Union[str, None] = None) -> ResponseT:
        """
        See https://redis.io/commands/ping
        """

        command: List = ["PING"]

        if message is not None:
            command.append(message)

        return self.execute(command)

    def echo(self, message: str) -> ResponseT:
        """
        See https://redis.io/commands/echo
        """

        command: List = ["ECHO", message]

        return self.execute(command)

    def copy(self, source: str, destination: str, replace: bool = False) -> ResponseT:
        """
        See https://redis.io/commands/copy
        """

        command: List = ["COPY", source, destination]

        if replace:
            command.append("REPLACE")

        return self.execute(command)

    def delete(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/del
        """

        if len(keys) == 0:
            raise Exception("At least one key must be deleted.")

        command: List = ["DEL", *keys]

        return self.execute(command)

    def exists(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/exists
        """

        if len(keys) == 0:
            raise Exception("At least one key must be checked.")

        command: List = ["EXISTS", *keys]

        return self.execute(command)

    def expire(self, key: str, seconds: int) -> ResponseT:
        """
        See https://redis.io/commands/expire
        """

        command: List = ["EXPIRE", key, seconds]

        return self.execute(command)

    def expireat(self, key: str, unix_time_seconds: int) -> ResponseT:
        """
        See https://redis.io/commands/expireat
        """

        command: List = ["EXPIREAT", key, unix_time_seconds]

        return self.execute(command)

    def keys(self, pattern: str) -> ResponseT:
        """
        See https://redis.io/commands/keys
        """

        command: List = ["KEYS", pattern]

        return self.execute(command)

    def persist(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/persist
        """

        command: List = ["PERSIST", key]

        return self.execute(command)

    def pexpire(self, key: str, milliseconds: int) -> ResponseT:
        """
        See https://redis.io/commands/pexpire
        """

        command: List = ["PEXPIRE", key, milliseconds]

        return self.execute(command)

    def pexpireat(self, key: str, unix_time_milliseconds: int) -> ResponseT:
        """
        See https://redis.io/commands/pexpireat
        """

        command: List = ["PEXPIREAT", key, unix_time_milliseconds]

        return self.execute(command)

    def pttl(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/pttl
        """

        command: List = ["PTTL", key]

        return self.execute(command)

    def randomkey(self) -> ResponseT:
        """
        See https://redis.io/commands/randomkey
        """

        command: List = ["RANDOMKEY"]

        return self.execute(command)

    def rename(self, key: str, newkey: str) -> ResponseT:
        """
        See https://redis.io/commands/rename
        """

        command: List = ["RENAME", key, newkey]

        return self.execute(command)

    def renamenx(self, key: str, newkey: str) -> ResponseT:
        """
        See https://redis.io/commands/renamenx
        """

        command: List = ["RENAMENX", key, newkey]

        return self.execute(command)

    def scan(
        self,
        cursor: int,
        match: Union[str, None] = None,
        count: Union[int, None] = None,
        type: Union[str, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/scan
        """

        command: List = ["SCAN", cursor]

        if match is not None:
            command.extend(["MATCH", match])

        if count is not None:
            command.extend(["COUNT", count])

        if type is not None:
            command.extend(["TYPE", type])

        # The raw result is composed of the new cursor and the List of elements.
        return self.execute(command)

    def touch(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/touch
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["TOUCH", *keys]

        return self.execute(command)

    def ttl(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/ttl
        """

        command: List = ["TTL", key]

        return self.execute(command)

    def type(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/type
        """

        command: List = ["TYPE", key]

        return self.execute(command)

    def unlink(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/unlink
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["UNLINK", *keys]

        return self.execute(command)

    def geoadd(
        self,
        key: str,
        *members: Tuple[float, float, str],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/geoadd

        :param members: a sequence of (longitude, latitude, name).
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
            command.extend(member)

        return self.execute(command)

    def geodist(
        self,
        key: str,
        member1: str,
        member2: str,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
    ) -> ResponseT:
        """
        See https://redis.io/commands/geodist
        """

        command: List = ["GEODIST", key, member1, member2, unit]

        return self.execute(command)

    def geohash(self, key: str, *members: str) -> ResponseT:
        """
        See https://redis.io/commands/geohash
        """

        command: List = ["GEOHASH", key, *members]

        return self.execute(command)

    def geopos(self, key: str, *members: str) -> ResponseT:
        """
        See https://redis.io/commands/geopos
        """

        command: List = ["GEOPOS", key, *members]

        return self.execute(command)

    def georadius(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        any: bool = False,
        order: Union[Literal["ASC", "DESC"], None] = None,
        store: Union[str, None] = None,
        storedist: Union[str, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/georadius
        """

        handle_georadius_write_exceptions(
            withdist, withhash, withcoord, count, any, store, storedist
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
            if any:
                command.append("ANY")

        if order:
            command.append(order)

        if store:
            command.extend(["STORE", store])

        if storedist:
            command.extend(["STOREDIST", storedist])

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def georadius_ro(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        any: bool = False,
        order: Union[Literal["ASC", "DESC"], None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/georadius_ro
        """

        if any and count is None:
            raise Exception('"any" can only be used together with "count".')

        command: List = ["GEORADIUS_RO", key, longitude, latitude, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if order:
            command.append(order)

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def georadiusbymember(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        any: bool = False,
        order: Union[Literal["ASC", "DESC"], None] = None,
        store: Union[str, None] = None,
        storedist: Union[str, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/georadiusbymember
        """

        handle_georadius_write_exceptions(
            withdist, withhash, withcoord, count, any, store, storedist
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
            if any:
                command.append("ANY")

        if order:
            command.append(order)

        if store:
            command.extend(["STORE", store])

        if storedist:
            command.extend(["STOREDIST", storedist])

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def georadiusbymember_ro(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        any: bool = False,
        order: Union[Literal["ASC", "DESC"], None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/georadiusbymember_ro
        """

        if any and count is None:
            raise Exception('"any" can only be used together with "count".')

        command: List = ["GEORADIUSBYMEMBER_RO", key, member, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if order:
            command.append(order)

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def geosearch(
        self,
        key: str,
        member: Union[str, None] = None,
        longitude: Union[float, None] = None,
        latitude: Union[float, None] = None,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        radius: Union[float, None] = None,
        width: Union[float, None] = None,
        height: Union[float, None] = None,
        order: Union[Literal["ASC", "DESC"], None] = None,
        count: Union[int, None] = None,
        any: bool = False,
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/geosearch
        """

        handle_geosearch_exceptions(
            member,
            longitude,
            latitude,
            radius,
            width,
            height,
            count,
            any,
        )

        command: List = ["GEOSEARCH", key]

        if member is not None:
            command.extend(["FROMMEMBER", member])

        if longitude is not None:
            command.extend(["FROMLONLAT", longitude, latitude])

        if radius is not None:
            command.extend(["BYRADIUS", radius])

        if width is not None:
            command.extend(["BYBOX", width, height])

        command.append(unit)

        if order:
            command.append(order)

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def geosearchstore(
        self,
        destination: str,
        source: str,
        member: Union[str, None] = None,
        longitude: Union[float, None] = None,
        latitude: Union[float, None] = None,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        radius: Union[float, None] = None,
        width: Union[float, None] = None,
        height: Union[float, None] = None,
        order: Union[Literal["ASC", "DESC"], None] = None,
        count: Union[int, None] = None,
        any: bool = False,
        storedist: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/geosearchstore
        """

        handle_geosearch_exceptions(
            member,
            longitude,
            latitude,
            radius,
            width,
            height,
            count,
            any,
        )

        command: List = ["GEOSEARCHSTORE", destination, source]

        if member is not None:
            command.extend(["FROMMEMBER", member])

        if longitude is not None:
            command.extend(["FROMLONLAT", longitude, latitude])

        if radius is not None:
            command.extend(["BYRADIUS", radius])

        if width is not None:
            command.extend(["BYBOX", width, height])

        command.append(unit)

        if order:
            command.append(order)

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if storedist:
            command.append("STOREDIST")

        return self.execute(command)

    def hdel(self, key: str, *fields: str) -> ResponseT:
        """
        See https://redis.io/commands/hdel
        """

        if len(fields) == 0:
            raise Exception("At least one field must be deleted.")

        command: List = ["HDEL", key, *fields]

        return self.execute(command)

    def hexists(self, key: str, field: str) -> ResponseT:
        """
        See https://redis.io/commands/hexists
        """

        command: List = ["HEXISTS", key, field]

        return self.execute(command)

    def hget(self, key: str, field: str) -> ResponseT:
        """
        See https://redis.io/commands/hget
        """

        command: List = ["HGET", key, field]

        return self.execute(command)

    def hgetall(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/hgetall
        """

        command: List = ["HGETALL", key]

        return self.execute(command)

    def hincrby(self, key: str, field: str, increment: int) -> ResponseT:
        """
        See https://redis.io/commands/hincrby
        """

        command: List = ["HINCRBY", key, field, increment]

        return self.execute(command)

    def hincrbyfloat(self, key: str, field: str, increment: float) -> ResponseT:
        """
        See https://redis.io/commands/hincrbyfloat
        """

        command: List = ["HINCRBYFLOAT", key, field, increment]

        return self.execute(command)

    def hkeys(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/hkeys
        """

        command: List = ["HKEYS", key]

        return self.execute(command)

    def hlen(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/hlen
        """

        command: List = ["HLEN", key]

        return self.execute(command)

    def hmget(self, key: str, *fields: str) -> ResponseT:
        """
        See https://redis.io/commands/hmget
        """

        if len(fields) == 0:
            raise Exception("At least one field must be specified.")

        command: List = ["HMGET", key, *fields]

        return self.execute(command)

    def hmset(self, key: str, values: Dict[str, str]) -> ResponseT:
        """
        See https://redis.io/commands/hmset
        """

        command: List = ["HMSET", key]

        for field, value in values.items():
            command.extend([field, value])

        return self.execute(command)

    def hrandfield(
        self, key: str, count: Union[int, None] = None, withvalues: bool = False
    ) -> ResponseT:
        """
        See https://redis.io/commands/hrandfield
        """

        if count is None and withvalues:
            raise Exception('"withvalues" can only be used together with "count"')

        command: List = ["HRANDFIELD", key]

        if count is not None:
            command.extend([count])

            if withvalues:
                command.append("WITHVALUES")

        return self.execute(command)

    def hscan(
        self,
        key: str,
        cursor: int,
        match: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/hscan
        """

        command: List = ["HSCAN", key, cursor]

        if match is not None:
            command.extend(["MATCH", match])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        return self.execute(command)

    def hset(
        self,
        key: str,
        field: Optional[str] = None,
        value: Optional[str] = None,
        values: Optional[Dict[str, str]] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/hset
        """
        command: List = ["HSET", key]

        if field is None and values is None:
            raise Exception("'hset' with no key value pairs")

        if field and value:
            command.extend([field, value])

        if values is not None:
            for field, value in values.items():
                command.extend([field, value])

        return self.execute(command)

    def hsetnx(self, key: str, field: str, value: str) -> ResponseT:
        """
        See https://redis.io/commands/hsetnx
        """

        command: List = ["HSETNX", key, field, value]

        return self.execute(command)

    def hstrlen(self, key: str, field: str) -> ResponseT:
        """
        See https://redis.io/commands/hstrlen
        """

        command: List = ["HSTRLEN", key, field]

        return self.execute(command)

    def hvals(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/hvals
        """

        command: List = ["HVALS", key]

        return self.execute(command)

    def pfadd(self, key: str, *elements: Any) -> ResponseT:
        """
        See https://redis.io/commands/pfadd
        """

        command: List = ["PFADD", key, *elements]

        return self.execute(command)

    def pfcount(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/pfcount
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["PFCOUNT", *keys]

        return self.execute(command)

    def pfmerge(self, destkey: str, *sourcekeys: str) -> ResponseT:
        """
        See https://redis.io/commands/pfmerge
        """

        command: List = ["PFMERGE", destkey, *sourcekeys]

        return self.execute(command)

    def lindex(self, key: str, index: int) -> ResponseT:
        """
        See https://redis.io/commands/lindex
        """

        command: List = ["LINDEX", key, index]

        return self.execute(command)

    def linsert(
        self,
        key: str,
        where: Literal["BEFORE", "AFTER"],
        pivot: str,
        element: str,
    ) -> ResponseT:
        """
        See https://redis.io/commands/linsert
        """

        command: List = ["LINSERT", key, where, pivot, element]

        return self.execute(command)

    def llen(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/llen
        """

        command: List = ["LLEN", key]

        return self.execute(command)

    def lmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["LEFT", "RIGHT"] = "LEFT",
        whereto: Literal["LEFT", "RIGHT"] = "RIGHT",
    ) -> ResponseT:
        """
        See https://redis.io/commands/lmove
        """

        command: List = [
            "LMOVE",
            source,
            destination,
            wherefrom,
            whereto,
        ]

        return self.execute(command)

    def lpop(self, key: str, count: Union[int, None] = None) -> ResponseT:
        """
        See https://redis.io/commands/lpop
        """

        command: List = ["LPOP", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def lpos(
        self,
        key: str,
        element: str,
        rank: Union[int, None] = None,
        count: Union[int, None] = None,
        maxlen: Union[int, None] = None,
    ) -> ResponseT:
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

        return self.execute(command)

    def lpush(self, key: str, *elements: str) -> ResponseT:
        """
        See https://redis.io/commands/lpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["LPUSH", key, *elements]

        return self.execute(command)

    def lpushx(self, key: str, *elements: str) -> ResponseT:
        """
        See https://redis.io/commands/lpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["LPUSHX", key, *elements]

        return self.execute(command)

    def lrange(self, key: str, start: int, stop: int) -> ResponseT:
        """
        See https://redis.io/commands/lrange
        """

        command: List = ["LRANGE", key, start, stop]

        return self.execute(command)

    def lrem(self, key: str, count: int, element: str) -> ResponseT:
        """
        See https://redis.io/commands/lrem
        """

        command: List = ["LREM", key, count, element]

        return self.execute(command)

    def lset(self, key: str, index: int, element: str) -> ResponseT:
        """
        See https://redis.io/commands/lset
        """

        command: List = ["LSET", key, index, element]

        return self.execute(command)

    def ltrim(self, key: str, start: int, stop: int) -> ResponseT:
        """
        See https://redis.io/commands/ltrim
        """

        command: List = ["LTRIM", key, start, stop]

        return self.execute(command)

    def rpop(self, key: str, count: Union[int, None] = None) -> ResponseT:
        """
        See https://redis.io/commands/rpop
        """

        command: List = ["RPOP", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def rpoplpush(self, source: str, destination: str) -> ResponseT:
        """
        See https://redis.io/commands/rpoplpush
        """

        command: List = ["RPOPLPUSH", source, destination]

        return self.execute(command)

    def rpush(self, key: str, *elements: str) -> ResponseT:
        """
        See https://redis.io/commands/rpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["RPUSH", key, *elements]

        return self.execute(command)

    def rpushx(self, key: str, *elements: Any) -> ResponseT:
        """
        See https://redis.io/commands/rpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["RPUSHX", key, *elements]

        return self.execute(command)

    def publish(self, channel: str, message: str) -> ResponseT:
        """
        See https://redis.io/commands/publish
        """

        command: List = ["PUBLISH", channel, message]

        return self.execute(command)

    def eval(
        self,
        script: str,
        keys: Union[List[str], None] = None,
        args: Union[List, None] = None,
    ) -> ResponseT:
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

        return self.execute(command)

    def evalsha(
        self,
        sha1: str,
        keys: Union[List[str], None] = None,
        args: Union[List, None] = None,
    ) -> ResponseT:
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

        return self.execute(command)

    def dbsize(self) -> ResponseT:
        """
        See https://redis.io/commands/dbsize
        """

        command: List = ["DBSIZE"]

        return self.execute(command)

    def flushall(
        self, flush_type: Union[Literal["ASYNC", "SYNC"], None] = None
    ) -> ResponseT:
        """
        See https://redis.io/commands/flushall
        """

        command: List = ["FLUSHALL"]

        if flush_type:
            command.append(flush_type)

        return self.execute(command)

    def flushdb(
        self, flush_type: Union[Literal["ASYNC", "SYNC"], None] = None
    ) -> ResponseT:
        """
        See https://redis.io/commands/flushdb
        """

        command: List = ["FLUSHDB"]

        if flush_type:
            command.append(flush_type)

        return self.execute(command)

    def time(self) -> ResponseT:
        """
        See https://redis.io/commands/time
        """

        command: List = ["TIME"]

        return self.execute(command)

    def sadd(self, key: str, *members: str) -> ResponseT:
        """
        See https://redis.io/commands/sadd
        """

        if len(members) == 0:
            raise Exception("At least one member must be added.")

        command: List = ["SADD", key, *members]

        return self.execute(command)

    def scard(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/scard
        """

        command: List = ["SCARD", key]

        return self.execute(command)

    def sdiff(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/sdiff
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SDIFF", *keys]

        return self.execute(command)

    def sdiffstore(self, destination: str, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/sdiffstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SDIFFSTORE", destination, *keys]

        return self.execute(command)

    def sinter(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/sinter
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SINTER", *keys]

        return self.execute(command)

    def sinterstore(self, destination: str, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/sinterstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SINTERSTORE", destination, *keys]

        return self.execute(command)

    def sismember(self, key: str, member: str) -> ResponseT:
        """
        See https://redis.io/commands/sismember
        """

        command: List = ["SISMEMBER", key, member]

        return self.execute(command)

    def smembers(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/smembers
        """

        command: List = ["SMEMBERS", key]

        return self.execute(command)

    def smismember(self, key: str, *members: str) -> ResponseT:
        """
        See https://redis.io/commands/smismember
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["SMISMEMBER", key, *members]

        return self.execute(command)

    def smove(self, source: str, destination: str, member: str) -> ResponseT:
        """
        See https://redis.io/commands/smove
        """

        command: List = ["SMOVE", source, destination, member]

        return self.execute(command)

    def spop(self, key: str, count: Union[int, None] = None) -> ResponseT:
        """
        See https://redis.io/commands/spop
        """

        command: List = ["SPOP", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def srandmember(self, key: str, count: Union[int, None] = None) -> ResponseT:
        """
        See https://redis.io/commands/srandmember
        """

        command: List = ["SRANDMEMBER", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def srem(self, key: str, *members: str) -> ResponseT:
        """
        See https://redis.io/commands/srem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["SREM", key, *members]

        return self.execute(command)

    def sscan(
        self,
        key: str,
        cursor: int = 0,
        match: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/sscan
        """

        command: List = ["SSCAN", key, cursor]

        if match is not None:
            command.extend(["MATCH", match])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        return self.execute(command)

    def sunion(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/sunion
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SUNION", *keys]

        return self.execute(command)

    def sunionstore(self, destination: str, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/sunionstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SUNIONSTORE", destination, *keys]

        return self.execute(command)

    def zadd(
        self,
        key: str,
        scores: Dict[str, float],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
        incr: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zadd
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

        for name, score in scores.items():
            command.extend([score, name])

        return self.execute(command)

    def zcard(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/zcard
        """

        command: List = ["ZCARD", key]

        return self.execute(command)

    def zcount(self, key: str, min: FloatMinMaxT, max: FloatMinMaxT) -> ResponseT:
        """
        See https://redis.io/commands/zcount

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: List = ["ZCOUNT", key, min, max]

        return self.execute(command)

    def zdiff(self, keys: List[str], withscores: bool = False) -> ResponseT:
        """
        See https://redis.io/commands/zdiff

        The number of keys is calculated automatically.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZDIFF", len(keys), *keys]

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zdiffstore(self, destination: str, keys: List[str]) -> ResponseT:
        """
        See https://redis.io/commands/zdiffstore

        The number of keys is calculated automatically.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZDIFFSTORE", destination, len(keys), *keys]

        return self.execute(command)

    def zincrby(self, key: str, increment: float, member: str) -> ResponseT:
        """
        See https://redis.io/commands/zincrby
        """

        command: List = ["ZINCRBY", key, increment, member]

        return self.execute(command)

    def zinter(
        self,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
        withscores: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zinter

        The number of keys is calculated automatically.
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

        return self.execute(command)

    def zinterstore(
        self,
        destination: str,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
    ) -> ResponseT:
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

        return self.execute(command)

    def zlexcount(self, key: str, min: str, max: str) -> ResponseT:
        """
        See https://redis.io/commands/zlexcount
        """

        if not min.startswith(("(", "[", "+", "-")) or not max.startswith(
            ("(", "[", "+", "-")
        ):
            raise Exception(
                "\"min\" and \"max\" must either start with '(' or '[' or be '+' or '-'."
            )

        command: List = ["ZLEXCOUNT", key, min, max]

        return self.execute(command)

    def zmscore(self, key: str, members: List[str]) -> ResponseT:
        """
        See https://redis.io/commands/zmscore
        """

        if len(members) == 0:
            raise Exception("At least one member must be specified.")

        command: List = ["ZMSCORE", key, *members]

        return self.execute(command)

    def zpopmax(self, key: str, count: Union[int, None] = None) -> ResponseT:
        """
        See https://redis.io/commands/zpopmax
        """

        command: List = ["ZPOPMAX", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def zpopmin(self, key: str, count: Union[int, None] = None) -> ResponseT:
        """
        See https://redis.io/commands/zpopmin
        """

        command: List = ["ZPOPMIN", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def zrandmember(
        self, key: str, count: Union[int, None] = None, withscores: bool = False
    ) -> ResponseT:
        """
        See https://redis.io/commands/zrandmember
        """

        if count is None and withscores:
            raise Exception('"withscores" can only be used with "count".')

        command: List = ["ZRANDMEMBER", key]

        if count is not None:
            command.append(count)

            if withscores:
                command.append("WITHSCORES")

        return self.execute(command)

    def zrange(
        self,
        key: str,
        start: FloatMinMaxT,
        stop: FloatMinMaxT,
        sortby: Union[Literal["BYSCORE", "BYLEX"], None] = None,
        rev: bool = False,
        offset: Union[int, None] = None,
        count: Union[int, None] = None,
        withscores: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zrange

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        handle_non_deprecated_zrange_exceptions(sortby, start, stop, offset, count)

        command: List = ["ZRANGE", key, start, stop]

        if sortby:
            command.append(sortby)

        if rev:
            command.append("REV")

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zrangebylex(
        self,
        key: str,
        min: str,
        max: str,
        offset: Union[int, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zrangebylex
        """

        handle_zrangebylex_exceptions(min, max, offset, count)

        command: List = ["ZRANGEBYLEX", key, min, max]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return self.execute(command)

    def zrangebyscore(
        self,
        key: str,
        min: FloatMinMaxT,
        max: FloatMinMaxT,
        withscores: bool = False,
        offset: Union[int, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zrangebyscore

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        if number_are_not_none(offset, count, number=1):
            raise Exception('Both "offset" and "count" must be specified.')

        command: List = ["ZRANGEBYSCORE", key, min, max]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zrangestore(
        self,
        dst: str,
        src: str,
        min: FloatMinMaxT,
        max: FloatMinMaxT,
        sortby: Union[Literal["BYSCORE", "BYLEX"], None] = None,
        rev: bool = False,
        offset: Union[int, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zrangestore
        """

        handle_non_deprecated_zrange_exceptions(sortby, min, max, offset, count)

        command: List = ["ZRANGESTORE", dst, src, min, max]

        if sortby:
            command.append(sortby)

        if rev:
            command.append("REV")

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return self.execute(command)

    def zrank(self, key: str, member: str) -> ResponseT:
        """
        See https://redis.io/commands/zrank
        """

        command: List = ["ZRANK", key, member]

        return self.execute(command)

    def zrem(self, key: str, *members: str) -> ResponseT:
        """
        See https://redis.io/commands/zrem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["ZREM", key, *members]

        return self.execute(command)

    def zremrangebylex(self, key: str, min: str, max: str) -> ResponseT:
        """
        See https://redis.io/commands/zremrangebylex
        """

        if not min.startswith(("(", "[", "+", "-")) or not max.startswith(
            ("(", "[", "+", "-")
        ):
            raise Exception(
                "\"min\" and \"max\" must either start with '(' or '[' or be '+' or '-'."
            )

        command: List = ["ZREMRANGEBYLEX", key, min, max]

        return self.execute(command)

    def zremrangebyrank(self, key: str, start: int, stop: int) -> ResponseT:
        """
        See https://redis.io/commands/zremrangebyrank
        """

        command: List = ["ZREMRANGEBYRANK", key, start, stop]

        return self.execute(command)

    def zremrangebyscore(
        self, key: str, min: FloatMinMaxT, max: FloatMinMaxT
    ) -> ResponseT:
        """
        See https://redis.io/commands/zremrangebyscore\

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: List = ["ZREMRANGEBYSCORE", key, min, max]

        return self.execute(command)

    def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> ResponseT:
        """
        See https://redis.io/commands/zrevrange
        """

        command: List = ["ZREVRANGE", key, start, stop]

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zrevrangebylex(
        self,
        key: str,
        max: str,
        min: str,
        offset: Union[int, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zrevrangebylex
        """

        handle_zrangebylex_exceptions(min, max, offset, count)

        command: List = ["ZREVRANGEBYLEX", key, max, min]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return self.execute(command)

    def zrevrangebyscore(
        self,
        key: str,
        max: FloatMinMaxT,
        min: FloatMinMaxT,
        withscores: bool = False,
        offset: Union[int, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zrevrangebyscore

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        if number_are_not_none(offset, count, number=1):
            raise Exception('Both "offset" and "count" must be specified.')

        command: List = ["ZREVRANGEBYSCORE", key, max, min]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zrevrank(self, key: str, member: str) -> ResponseT:
        """
        See https://redis.io/commands/zrevrank
        """

        command: List = ["ZREVRANK", key, member]

        return self.execute(command)

    def zscan(
        self,
        key: str,
        cursor: int,
        match: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zscan
        """

        command: List = ["ZSCAN", key, cursor]

        if match is not None:
            command.extend(["MATCH", match])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        return self.execute(command)

    def zscore(self, key: str, member: str) -> ResponseT:
        """
        See https://redis.io/commands/zscore
        """

        command: List = ["ZSCORE", key, member]

        return self.execute(command)

    def zunion(
        self,
        keys: List[str],
        weights: Union[List[float], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
        withscores: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/zunion

        The number of keys is calculated automatically.
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

        return self.execute(command)

    def zunionstore(
        self,
        destination: str,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
    ) -> ResponseT:
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

        return self.execute(command)

    def append(self, key: str, value: str) -> ResponseT:
        """
        See https://redis.io/commands/append
        """

        command: List = ["APPEND", key, value]

        return self.execute(command)

    def decr(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/decr
        """

        command: List = ["DECR", key]

        return self.execute(command)

    def decrby(self, key: str, decrement: int) -> ResponseT:
        """
        See https://redis.io/commands/decrby
        """

        command: List = ["DECRBY", key, decrement]

        return self.execute(command)

    def get(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/get
        """

        command: List = ["GET", key]

        return self.execute(command)

    def getdel(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/getdel
        """

        command: List = ["GETDEL", key]

        return self.execute(command)

    def getex(
        self,
        key: str,
        ex: Union[int, None] = None,
        px: Union[int, None] = None,
        exat: Union[int, None] = None,
        pxat: Union[int, None] = None,
        persist: Union[bool, None] = None,
    ) -> ResponseT:
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

        return self.execute(command)

    def getrange(self, key: str, start: int, end: int) -> ResponseT:
        """
        See https://redis.io/commands/getrange
        """

        command: List = ["GETRANGE", key, start, end]

        return self.execute(command)

    def getset(self, key: str, value: str) -> ResponseT:
        """
        See https://redis.io/commands/getset
        """

        command: List = ["GETSET", key, value]

        return self.execute(command)

    def incr(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/incr
        """

        command: List = ["INCR", key]

        return self.execute(command)

    def incrby(self, key: str, increment: int) -> ResponseT:
        """
        See https://redis.io/commands/incrby
        """

        command: List = ["INCRBY", key, increment]

        return self.execute(command)

    def incrbyfloat(self, key: str, increment: float) -> ResponseT:
        """
        See https://redis.io/commands/incrbyfloat
        """

        command: List = ["INCRBYFLOAT", key, increment]

        return self.execute(command)

    def mget(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/mget
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["MGET", *keys]

        return self.execute(command)

    def mset(self, values: Dict[str, str]) -> ResponseT:
        """
        See https://redis.io/commands/mset
        """

        command: List = ["MSET"]

        for key, value in values.items():
            command.extend([key, value])

        return self.execute(command)

    def msetnx(self, values: Dict[str, str]) -> ResponseT:
        """
        See https://redis.io/commands/msetnx
        """

        command: List = ["MSETNX"]

        for key, value in values.items():
            command.extend([key, value])

        return self.execute(command)

    def psetex(self, key: str, milliseconds: int, value: str) -> ResponseT:
        """
        See https://redis.io/commands/psetex
        """

        command: List = ["PSETEX", key, milliseconds, value]

        return self.execute(command)

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
    ) -> ResponseT:
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

        return self.execute(command)

    def setex(self, key: str, seconds: int, value: str) -> ResponseT:
        """
        See https://redis.io/commands/setex
        """

        command: List = ["SETEX", key, seconds, value]

        return self.execute(command)

    def setnx(self, key: str, value: str) -> ResponseT:
        """
        See https://redis.io/commands/setnx
        """

        command: List = ["SETNX", key, value]

        return self.execute(command)

    def setrange(self, key: str, offset: int, value: str) -> ResponseT:
        """
        See https://redis.io/commands/setrange
        """

        command: List = ["SETRANGE", key, offset, value]

        return self.execute(command)

    def strlen(self, key: str) -> ResponseT:
        """
        See https://redis.io/commands/strlen
        """

        command: List = ["STRLEN", key]

        return self.execute(command)

    def substr(self, key: str, start: int, end: int) -> ResponseT:
        """
        See https://redis.io/commands/substr
        """

        command: List = ["SUBSTR", key, start, end]

        return self.execute(command)

    def script_exists(self, *sha1: str) -> ResponseT:
        """
        See https://redis.io/commands/script-exists
        """

        if len(sha1) == 0:
            raise Exception("At least one sha1 digests must be provided.")

        command: List = ["SCRIPT", "EXISTS", *sha1]

        return self.execute(command)

    def script_flush(
        self, flush_type: Optional[Literal["ASYNC", "SYNC"]] = None
    ) -> ResponseT:
        """
        See https://redis.io/commands/script-flush
        """

        command: List = ["SCRIPT", "FLUSH"]

        if flush_type:
            command.append(flush_type)

        return self.execute(command)

    def script_load(self, script: str) -> ResponseT:
        """
        See https://redis.io/commands/script-load
        """

        command: List = ["SCRIPT", "LOAD", script]

        return self.execute(command)


# It doesn't inherit from "Redis" mainly because of the methods signatures.
class BitFieldCommands:
    def __init__(self, client: Commands, key: str):
        self.client = client
        self.command: List = ["BITFIELD", key]

    def get(self, encoding: str, offset: Union[int, str]) -> "BitFieldCommands":
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    def set(
        self, encoding: str, offset: Union[int, str], value: int
    ) -> "BitFieldCommands":
        """
        Set the specified bit field and returns its old value.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["SET", encoding, offset, value]
        self.command.extend(_command)

        return self

    def incrby(
        self, encoding: str, offset: Union[int, str], increment: int
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

    def execute(self) -> ResponseT:
        return self.client.execute(command=self.command)


class BitFieldROCommands:
    def __init__(self, client: Commands, key: str):
        self.client = client
        self.command: List = ["BITFIELD_RO", key]

    def get(self, encoding: str, offset: Union[int, str]) -> "BitFieldROCommands":
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield_ro
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    def execute(self) -> ResponseT:
        return self.client.execute(command=self.command)


AsyncCommands = Commands
AsyncBitFieldCommands = BitFieldCommands
AsyncBitFieldROCommands = BitFieldROCommands
