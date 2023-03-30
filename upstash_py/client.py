from upstash_py.http.execute import execute
from upstash_py.schema.http import RESTResult, RESTEncoding
from upstash_py.config import config
from upstash_py.utils.format import (
    format_geo_positions,
    format_geo_members_return,
    format_hash,
    format_pubsub_numsub,
    format_bool_list,
    format_time_output,
    format_sorted_set,
)
from aiohttp import ClientSession
from typing import Type, Any, Self, Literal
from upstash_py.schema.commands.parameters import BitFieldOffset, GeoMember
from upstash_py.schema.commands.returns import (
    GeoMembersReturn,
    FormattedGeoMembersReturn,
    HashReturn,
    FormattedHashReturn,
    SortedSetReturn,
    FormattedSortedSetReturn
)


class Redis:
    def __init__(
        self,
        url: str,
        token: str,
        enable_telemetry: bool = config["ENABLE_TELEMETRY"],
        rest_encoding: RESTEncoding = config["REST_ENCODING"],
        rest_retries: int = config["REST_RETRIES"],
        rest_retry_interval: int = config["REST_RETRY_INTERVAL"],
        allow_deprecated: bool = config["ALLOW_DEPRECATED"],
        format_return: bool = config["FORMAT_RETURN"]
    ):
        self.url = url
        self.token = token
        self.enable_telemetry = enable_telemetry
        self.allow_deprecated = allow_deprecated
        self.format_return = format_return

        # If the encoding is set as "True", it defaults to config.
        self.rest_encoding = config["REST_ENCODING"] if rest_encoding else rest_encoding
        self.rest_retries = rest_retries
        self.rest_retry_interval = rest_retry_interval

        self._session: ClientSession | None = None

    async def __aenter__(self) -> ClientSession:
        """
        Enter the async context.
        """

        self._session = ClientSession()
        # It needs to return the session object because it will be used in "async with" statements.
        return self._session

    async def __aexit__(self, exc_type: Type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        """
        Exit the async context.
        """

        await self._session.close()

    async def run(self, command: list) -> RESTResult:
        """
        Specify the http options and execute the command.
        """

        return await execute(
            session=self._session,
            url=self.url,
            token=self.token,
            encoding=self.rest_encoding,
            retries=self.rest_retries,
            retry_interval=self.rest_retry_interval,
            command=command,
        )

    async def bitcount(self, key: str, start: int | None = None, end: int | None = None) -> int:
        """
        See https://redis.io/commands/bitcount
        """

        if (start is None and end is not None) or (start is not None and end is None):
            raise Exception(
                """
                Both the start and end must be specified.
                """
            )

        command: list = ["BITCOUNT", key]

        if start is not None and end is not None:
            command.extend([start, end])

        return await self.run(command=command)

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

    async def bitop(
        self,
        operation: Literal["AND", "OR", "XOR", "NOT"],
        destination_key: str,
        *source_keys: str
    ) -> int:
        """
        See https://redis.io/commands/bitop
        """

        if operation == "NOT" and len(source_keys) > 1:
            raise Exception(
                """
                The "NOT" operation takes only one source key as argument.
                """
            )

        command: list = ["BITOP", operation, destination_key, *source_keys]

        return await self.run(command=command)

    async def bitpos(self, key: str, bit: Literal[0, 1], start: int | None = None, end: int | None = None) -> int:
        """
        See https://redis.io/commands/bitpos
        """

        if start is None and end is not None:
            raise Exception(
                """
                The end is specified, but the start is missing.
                """
            )

        command: list = ["BITPOS", key, bit]

        if start:
            command.append(start)

        if end:
            command.append(end)

        return await self.run(command=command)

    async def getbit(self, key: str, offset: int) -> int:
        """
        See https://redis.io/commands/getbit
        """

        command: list = ["GETBIT", key, offset]

        return await self.run(command=command)

    async def setbit(self, key: str, offset: int, value: Literal[0, 1]) -> int:
        """
        See https://redis.io/commands/setbit
        """

        command: list = ["SETBIT", key, offset, value]

        return await self.run(command=command)

    async def ping(self, message: str) -> str:
        """
        See https://redis.io/commands/ping
        """

        command: list = ["PING"]

        if message is not None:
            command.append(message)

        return await self.run(command=command)

    async def echo(self, message: str) -> str:
        """
        See https://redis.io/commands/echo
        """

        command: list = ["ECHO", message]

        return await self.run(command=command)

    async def copy(self, source: str, destination: str, replace: bool = False) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/copy
        """

        command: list = ["COPY", source, destination]

        if replace:
            command.append("REPLACE")

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def delete(self, *keys: str) -> int:
        """
        See https://redis.io/commands/del
        """

        command: list = ["DEL", *keys]

        return await self.run(command=command)

    async def exists(self, *keys: str) -> int:
        """
        See https://redis.io/commands/exists
        """

        command: list = ["EXISTS", *keys]

        return await self.run(command=command)

    async def expire(self, key: str, seconds: int) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/expire
        """

        command: list = ["EXPIRE", key, seconds]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def expireat(self, key: str, unix_time_seconds: int) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/expireat
        """

        command: list = ["EXPIREAT", key, unix_time_seconds]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def keys(self, pattern: str) -> list[str]:
        """
        See https://redis.io/commands/keys
        """

        command: list = ["KEYS", pattern]

        return await self.run(command=command)

    async def persist(self, key: str) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/persist
        """

        command: list = ["PERSIST", key]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def pexpire(self, key: str, milliseconds: int) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/pexpire
        """

        command: list = ["PEXPIRE", key, milliseconds]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def pexpireat(self, key: str, unix_time_milliseconds: int) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/pexpireat
        """

        command: list = ["EXPIREAT", key, unix_time_milliseconds]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def pttl(self, key: str) -> int:
        """
        See https://redis.io/commands/pttl
        """

        command: list = ["PTTL", key]

        return await self.run(command=command)

    async def randomkey(self) -> str | None:
        """
        See https://redis.io/commands/randomkey
        """

        command: list = ["RANDOMKEY"]

        return await self.run(command=command)

    async def rename(self, key: str, new_key: str) -> str:
        """
        See https://redis.io/commands/rename
        """

        command: list = ["RENAME", key, new_key]

        return await self.run(command=command)

    async def renamenx(self, key: str, new_key: str) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/renamenx
        """

        command: list = ["RENAMENX", key, new_key]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def scan(
        self,
        cursor: int,
        pattern: str | None = None,
        count: int | None = None,
        scan_type: str | None = None,
        return_cursor: bool = True
    ) -> list[int | list] | list:
        """
        See https://redis.io/commands/scan

        "MATCH" was replaced with "pattern".

        "TYPE" was replaced with "scan_type".

        If "return_cursor" is False, it won't return the cursor.
        """

        command: list = ["SCAN", cursor]

        if pattern is not None:
            command.extend(["MATCH", pattern])

        if count is not None:
            command.extend(["COUNT", count])

        if scan_type is not None:
            command.extend(["TYPE", scan_type])

        raw: list[int, list] = await self.run(command=command)

        # The raw result is composed of the new cursor and the array of elements.
        return raw[1] if not return_cursor else raw

    async def touch(self, *keys: str) -> int:
        """
        See https://redis.io/commands/touch
        """

        command: list = ["TOUCH", *keys]

        return await self.run(command=command)

    async def ttl(self, key: str) -> int:
        """
        See https://redis.io/commands/ttl
        """

        command: list = ["TTL", key]

        return await self.run(command=command)

    async def type(self, key: str) -> str | None:
        """
        See https://redis.io/commands/type
        """

        command: list = ["TYPE", key]

        return await self.run(command=command)

    async def unlink(self, *keys: str) -> int:
        """
        See https://redis.io/commands/unlink
        """

        command: list = ["UNLINK", *keys]

        return await self.run(command=command)

    async def geoadd(
        self,
        key: str,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        *members: GeoMember
    ) -> int:
        """
        See https://redis.io/commands/geoadd

        The members should be added as a sequence of GeoMember dict types (longitude, latitude, name).
        """

        if nx and xx:
            raise Exception(
                """
                "NX" and "XX" are mutually exclusive.
                """
            )

        command: list = ["GEOADD", key]

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        if ch:
            command.append("CH")

        for member in members:
            command.extend([member["longitude"], member["latitude"], member["name"]])

        return await self.run(command=command)

    async def geodist(
        self,
        key: str,
        first_member: str,
        second_member: str,
        unit: Literal["M", "KM", "FT", "MI"] = "M"
    ) -> str | None:  # Can be a double represented as string.
        """
        See https://redis.io/commands/geodist

        The measuring unit can be specified with "unit".
        """

        command: list = ["GEODIST", key, first_member, second_member, unit]

        return await self.run(command=command)

    async def geohash(self, key: str, *members: str) -> list[str | None]:
        """
        See https://redis.io/commands/geohash
        """

        command: list = ["GEOHASH", key, *members]

        return await self.run(command=command)

    async def geopos(
        self,
        key: str,
        *members: str,
    ) -> list[str | None] | list[dict[str, float] | None]:
        """
        See https://redis.io/commands/geopos

        If "format_return" is True, it will return the result as a dict.
        """

        command: list = ["GEOPOS", key, *members]

        raw: list[str | None] = await self.run(command=command)

        return format_geo_positions(raw=raw) if self.format_return else raw

    async def georadius(
        self,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        with_distance: bool = False,
        with_hash: bool = False,
        with_coordinates: bool = False,
        count: int | None = None,
        count_any: bool = False,
        sort: Literal["ASC", "DESC"] | None = None,
        store_as: str | None = None,
        store_distance_as: str | None = None
    ) -> GeoMembersReturn | FormattedGeoMembersReturn:
        """
        See https://redis.io/commands/georadius

        The measuring unit can be specified with "unit".

        "ANY" was replaced with "count_any".

        The sorting options can be specified with "sort".

        "[STORE and STORE_DIST] key" are written as "store_as" and "store_distance_as".
        """

        if not self.allow_deprecated:
            raise Exception(
                """
                As of Redis version 6.2.0, this command is regarded as deprecated.
                It can be replaced by "GEOSEARCH" and "GEOSEARCHSTORE" with the "radius" argument.
                
                Source: https://redis.io/commands/georadius
                """
            )

        if count_any and count is None:
            raise Exception(
                """
                "count_any" can only be used together with "count".
                """
            )

        command: list = ["GEORADIUS", longitude, latitude, radius, unit]

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        if count:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        if store_as:
            command.extend(["STORE", store_as])

        if store_distance_as:
            command.extend(["STOREDIST", store_distance_as])

        raw: GeoMembersReturn = await self.run(command=command)

        return format_geo_members_return(raw=raw) if self.format_return else raw

    async def georadius_ro(
        self,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        with_distance: bool = False,
        with_hash: bool = False,
        with_coordinates: bool = False,
        count: int | None = None,
        count_any: bool = False,
        sort: Literal["ASC", "DESC"] | None = None,
    ) -> GeoMembersReturn | FormattedGeoMembersReturn:
        """
        See https://redis.io/commands/georadius_ro

        The measuring unit can be specified with "unit".

        "ANY" was replaced with "count_any".

        The sorting options can be specified with "sort".
        """

        if not self.allow_deprecated:
            raise Exception(
                """
                As of Redis version 6.2.0, this command is regarded as deprecated.
                It can be replaced by "GEOSEARCH" with the "radius" argument.

                Source: https://redis.io/commands/georadius_ro
                """
            )

        if count_any and count is None:
            raise Exception(
                """
                "count_any" can only be used together with "count".
                """
            )

        command: list = ["GEORADIUS_RO", longitude, latitude, radius, unit]

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        if count:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        raw: GeoMembersReturn = await self.run(command=command)

        return format_geo_members_return(raw=raw) if self.format_return else raw

    async def georadiusbymember(
        self,
        member: str,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        with_distance: bool = False,
        with_hash: bool = False,
        with_coordinates: bool = False,
        count: int | None = None,
        count_any: bool = False,
        sort: Literal["ASC", "DESC"] | None = None,
        store_as: str | None = None,
        store_distance_as: str | None = None
    ) -> GeoMembersReturn | FormattedGeoMembersReturn:
        """
        See https://redis.io/commands/georadiusbymember

        The measuring unit can be specified with "unit".

        "ANY" was replaced with "count_any".

        The sorting options can be specified with "sort".

        "[STORE and STORE_DIST] key" are written as "store_as" and "store_distance_as".
        """

        if not self.allow_deprecated:
            raise Exception(
                """
                As of Redis version 6.2.0, this command is regarded as deprecated.
                It can be replaced by "GEOSEARCH" and "GEOSEARCHSTORE" with the "radius" and "member" arguments.

                Source: https://redis.io/commands/georadiusbymember
                """
            )

        if count_any and count is None:
            raise Exception(
                """
                "count_any" can only be used together with "count".
                """
            )

        command: list = ["GEORADIUSBYMEMBER", member, radius, unit]

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        if count:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        if store_as:
            command.extend(["STORE", store_as])

        if store_distance_as:
            command.extend(["STOREDIST", store_distance_as])

        raw: GeoMembersReturn = await self.run(command=command)

        return format_geo_members_return(raw=raw) if self.format_return else raw

    async def georadiusbymember_ro(
        self,
        member: str,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        with_distance: bool = False,
        with_hash: bool = False,
        with_coordinates: bool = False,
        count: int | None = None,
        count_any: bool = False,
        sort: Literal["ASC", "DESC"] | None = None,
    ) -> GeoMembersReturn | FormattedGeoMembersReturn:
        """
        See https://redis.io/commands/georadiusbymember

        The measuring unit can be specified with "unit".

        "ANY" was replaced with "count_any".

        The sorting options can be specified with "sort".
        """

        if not self.allow_deprecated:
            raise Exception(
                """
                As of Redis version 6.2.0, this command is regarded as deprecated.
                It can be replaced by "GEOSEARCH" with the "radius" and "member" arguments.

                Source: https://redis.io/commands/georadiusbymember_ro
                """
            )

        if count_any and count is None:
            raise Exception(
                """
                "count_any" can only be used together with "count".
                """
            )

        command: list = ["GEORADIUSBYMEMBER_RO", member, radius, unit]

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        if count:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        raw: GeoMembersReturn = await self.run(command=command)

        return format_geo_members_return(raw=raw) if self.format_return else raw

    async def geosearch(
        self,
        key: str,
        member: str | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        sort: Literal["ASC", "DESC"] | None = None,
        count: int | None = None,
        count_any: bool = False,
        with_distance: bool = False,
        with_hash: bool = False,
        with_coordinates: bool = False,
    ) -> GeoMembersReturn | FormattedGeoMembersReturn:
        """
        See https://redis.io/commands/geosearch

        "FROMMEMBER" was replaced with "member".

        "FROMLONLAT" was replaced with "longitude" and "latitude".

        "BYRADIUS" was replaced with "radius".

        "BYBOX" was replaced with "width" and "height".

        The measuring unit can be specified with "unit".

        The sorting options can be specified with "sort".

        "ANY" was replaced with "count_any".
        """

        if (
            member is not None
            and longitude is not None
            and latitude is not None
            ) or (
              member is None
              and longitude is None
              and latitude is None
        ):
            raise Exception(
                """
                Specify either the member's name with "member", 
                or the longitude and latitude with "longitude" and "latitude", but not both.
                """
            )

        if (
            radius is not None
            and width is not None
            and height is not None
            ) or (
              radius is None
              and width is None
              and height is None
        ):
            raise Exception(
                """
                Specify either the radius with "radius", 
                or the width and height with "width" and "height", but not both.
                """
            )

        if count_any and count is None:
            raise Exception(
                """
                "count_any" can only be used together with "count".
                """
            )

        command: list = ["GEOSEARCH", key]

        if member is not None:
            command.extend(["FROMMEMBER", member])

        if longitude is not None and latitude is not None:
            command.extend(["FROMLONLAT", longitude, latitude])

        if radius is not None:
            command.extend(["BYRADIUS", radius])

        if width is not None and height is not None:
            command.extend(["BYBOX", width, height])

        command.append(unit)

        if sort:
            command.append(sort)

        if count:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        raw: GeoMembersReturn = await self.run(command=command)

        return format_geo_members_return(raw=raw) if self.format_return else raw

    async def geosearchstore(
        self,
        destination_key: str,
        source_key: str,
        member: str | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        sort: Literal["ASC", "DESC"] | None = None,
        count: int | None = None,
        count_any: bool = False,
        store_distance: bool = False,
    ) -> int:
        """
        See https://redis.io/commands/geosearchstore

        "FROMMEMBER" was replaced with "member".

        "FROMLONLAT" was replaced with "longitude" and "latitude".

        "BYRADIUS" was replaced with "radius".

        "BYBOX" was replaced with "width" and "height".

        The measuring unit can be specified with "unit".

        "ASC" and "DESC" are written as sort.

        "ANY" was replaced with "count_any".
        """

        if (
            member is not None
            and longitude is not None
            and latitude is not None
            ) or (
              member is None
              and longitude is None
              and latitude is None
        ):
            raise Exception(
                """
                Specify either the member's name with "member", 
                or the longitude and latitude with "longitude" and "latitude", but not both.
                """
            )

        if (
            radius is not None
            and width is not None
            and height is not None
            ) or (
              radius is None
              and width is None
              and height is None
        ):
            raise Exception(
                """
                Specify either the radius with "radius", 
                or the width and height with "width" and "height", but not both.
                """
            )

        if count_any and count is None:
            raise Exception(
                """
                "count_any" can only be used together with "count".
                """
            )

        command: list = ["GEOSEARCHSTORE", destination_key, source_key]

        if member is not None:
            command.extend(["FROMMEMBER", member])

        if longitude is not None and latitude is not None:
            command.extend(["FROMLONLAT", longitude, latitude])

        if radius is not None:
            command.extend(["BYRADIUS", radius])

        if width is not None and height is not None:
            command.extend(["BYBOX", width, height])

        command.append(unit)

        if sort:
            command.append(sort)

        if count:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if store_distance:
            command.append("STOREDIST")

        return await self.run(command=command)

    async def hdel(self, key: str, *fields: str) -> int:
        """
        See https://redis.io/commands/hdel
        """

        command: list = ["HDEL", key, *fields]

        return await self.run(command=command)

    async def hexists(self, key: str, field: str) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/hexists
        """

        command: list = ["HEXISTS", key, field]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def hget(self, key: str, field: str) -> str | None:
        """
        See https://redis.io/commands/hget
        """

        command: list = ["HGET", key, field]

        return await self.run(command=command)

    async def hgetall(self, key: str) -> HashReturn | FormattedHashReturn:
        """
        See https://redis.io/commands/hgetall
        """

        command: list = ["HGETALL", key]

        raw: HashReturn = await self.run(command=command)

        return format_hash(raw=raw) if self.format_return else raw

    async def hincrby(self, key: str, field: str, increment: int) -> int:
        """
        See https://redis.io/commands/hincrby
        """

        command: list = ["HINCRBY", key, field, increment]

        return await self.run(command=command)

    async def hincrbyfloat(self, key: str, field: str, increment: float) -> str | float:
        """
        See https://redis.io/commands/hincrbyfloat
        """

        command: list = ["HINCRBYFLOAT", key, field, increment]

        raw: str = await self.run(command=command)

        return float(raw) if self.format_return else raw

    async def hkeys(self, key: str) -> list[str]:
        """
        See https://redis.io/commands/hkeys
        """

        command: list = ["HKEYS", key]

        return await self.run(command=command)

    async def hlen(self, key: str) -> int:
        """
        See https://redis.io/commands/hlen
        """

        command: list = ["HLEN", key]

        return await self.run(command=command)

    async def hmget(self, key: str, *fields: str) -> list[str | None]:
        """
        See https://redis.io/commands/hmget
        """

        command: list = ["HMGET", key, *fields]

        return await self.run(command=command)

    async def hmset(self, key: str, fields_and_values: dict) -> str:
        """
        See https://redis.io/commands/hmset
        """

        if not self.allow_deprecated:
            raise Exception(
                """
                As of Redis version 4.0.0, this command is regarded as deprecated.
                It can be replaced by "HSET".
                
                Source: https://redis.io/commands/hmset
                """
            )

        command: list = ["HMSET", key]

        for field, value in fields_and_values.items():
            command.extend([field, value])

        return await self.run(command=command)

    async def hrandfield(
        self,
        key: str,
        count: int = None,
        with_values: bool = False
    ) -> (str | None) | (HashReturn | FormattedHashReturn):
        """
        See https://redis.io/commands/hrandfield
        """

        if count is None and with_values:
            raise Exception(
                """
                "with_values" can only be used together with "count".
                """
            )

        command: list = ["HRANDFIELD", key]

        if count:
            command.extend(["COUNT", count])

        if with_values:
            command.append("WITHVALUES")

            raw: HashReturn = await self.run(command=command)

            return format_hash(raw=raw) if self.format_return else raw

        return await self.run(command=command)

    async def hscan(
        self,
        key: str,
        cursor: int,
        pattern: str = None,
        count: int = None,
        return_cursor: bool = True
    ) -> (list[int | HashReturn] | list[int | FormattedHashReturn]) | (HashReturn | FormattedHashReturn):
        """
        See https://redis.io/commands/hscan

        "MATCH" was replaced with "pattern".

        If "return_cursor" is False, it won't return the cursor.
        """

        command: list = ["HSCAN", key, cursor]

        if pattern:
            command.extend(["MATCH", pattern])

        if count:
            command.extend(["COUNT", count])

        raw: list[int | HashReturn] | HashReturn = await self.run(command=command)

        if return_cursor:
            return [raw[0], format_hash(raw=raw[1])] if self.format_return else raw

        # The raw result is composed of the new cursor and the array of elements.
        return format_hash(raw=raw[1]) if self.format_return else raw[1]

    async def hset(self, key: str, fields_and_values: dict) -> int:
        """
        See https://redis.io/commands/hset
        """

        command: list = ["HSET", key]

        for field, value in fields_and_values.items():
            command.extend([field, value])

        return await self.run(command=command)

    async def hsetnx(self, key: str, field: str, value: Any) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/hsetnx
        """

        command: list = ["HSETNX", key, field, value]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def hstrlen(self, key: str, field: str) -> int:
        """
        See https://redis.io/commands/hstrlen
        """

        command: list = ["HSTRLEN", key, field]

        return await self.run(command=command)

    async def hvals(self, key: str) -> list[str]:
        """
        See https://redis.io/commands/hvals
        """

        command: list = ["HVALS", key]

        return await self.run(command=command)

    async def pfadd(self, key: str, *elements: Any) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/pfadd
        """

        command: list = ["PFADD", key, *elements]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def pfcount(self, *keys: str) -> int:
        """
        See https://redis.io/commands/pfcount
        """

        command: list = ["PFCOUNT", *keys]

        return await self.run(command=command)

    async def pfmerge(self, destination_key: str, *source_keys: str) -> str:
        """
        See https://redis.io/commands/pfmerge
        """

        command: list = ["PFMERGE", destination_key, *source_keys]

        return await self.run(command=command)

    async def lindex(self, key: str, index: int) -> str | None:
        """
        See https://redis.io/commands/lindex
        """

        command: list = ["LINDEX", key, index]

        return await self.run(command=command)

    async def linsert(
        self,
        key: str,
        position: Literal["BEFORE", "AFTER"],
        pivot: Any,
        element: Any
    ) -> int:
        """
        See https://redis.io/commands/linsert

        The positioning can be specified with "position".
        """

        command: list = ["LINSERT", key, position, pivot, element]

        return await self.run(command=command)

    async def llen(self, key: str) -> int:
        """
        See https://redis.io/commands/llen
        """

        command: list = ["LLEN", key]

        return await self.run(command=command)

    async def lmove(
        self,
        source_key: str,
        destination_key: str,
        source_position: Literal["LEFT", "RIGHT"],
        destination_position: Literal["LEFT", "RIGHT"]
    ) -> str | None:
        """
        See https://redis.io/commands/lmove

        The positioning can be specified with "source_position" and "destination_position".
        """

        command: list = ["LMOVE", source_key, destination_key, source_position, destination_position]

        return await self.run(command=command)

    async def lpop(self, key: str, count: int = None) -> str | list[str] | None:
        """
        See https://redis.io/commands/lpop

        If "count" is specified, it will return a list of values.
        """

        command: list = ["LPOP", key]

        if count:
            command.append(count)

        return await self.run(command=command)

    async def lpos(
        self,
        key: str,
        element: Any,
        first_return: int = None,
        count: int = None,
        max_number_of_comparisons: int = None,
    ) -> (int | None) | list[int]:
        """
        See https://redis.io/commands/lpos

        "RANK" was replaced with "first_return".
        When set, it will return the positions starting from the specified occurrence.

        "MAXLEN" was replaced with "max_number_of_comparisons".
        """

        command: list = ["LPOS", key, element]

        if first_return:
            command.extend(["RANK", first_return])

        if count:
            command.extend(["COUNT", count])

        if max_number_of_comparisons:
            command.extend(["MAXLEN", max_number_of_comparisons])

        return await self.run(command=command)

    async def lpush(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/lpush
        """

        command: list = ["LPUSH", key, *elements]

        return await self.run(command=command)

    async def lpushx(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/lpushx
        """

        command: list = ["LPUSHX", key, *elements]

        return await self.run(command=command)

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        """
        See https://redis.io/commands/lrange
        """

        command: list = ["LRANGE", key, start, stop]

        return await self.run(command=command)

    async def lrem(self, key: str, count: int, element: Any) -> int:
        """
        See https://redis.io/commands/lrem
        """

        command: list = ["LREM", key, count, element]

        return await self.run(command=command)

    async def lset(self, key: str, index: int, element: Any) -> str:
        """
        See https://redis.io/commands/lset
        """

        command: list = ["LSET", key, index, element]

        return await self.run(command=command)

    async def ltrim(self, key: str, start: int, stop: int) -> str:
        """
        See https://redis.io/commands/ltrim
        """

        command: list = ["LTRIM", key, start, stop]

        return await self.run(command=command)

    async def rpop(self, key: str, count: int = None) -> str | list[str] | None:
        """
        See https://redis.io/commands/rpop

        If "count" is specified, it will return a list of values.
        """

        command: list = ["RPOP", key]

        if count:
            command.append(count)

        return await self.run(command=command)

    async def rpoplpush(self, source_key: str, destination_key: str) -> str | None:
        """
        See https://redis.io/commands/rpoplpush
        """

        if not self.allow_deprecated:
            raise Exception(
                """
                As of Redis version 6.2.0, this command is regarded as deprecated.
                It can be replaced by "LMOVE" with 
                "source_position" set to "RIGHT" and the "destination_position" set to "LEFT".

                Source: https://redis.io/commands/rpoplpush
                """
            )

        command: list = ["RPOPLPUSH", source_key, destination_key]

        return await self.run(command=command)

    async def rpush(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/rpush
        """

        command: list = ["RPUSH", key, *elements]

        return await self.run(command=command)

    async def rpushx(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/rpushx
        """

        command: list = ["RPUSHX", key, *elements]

        return await self.run(command=command)

    async def publish(self, channel: str, message: str) -> int:
        """
        See https://redis.io/commands/publish
        """

        command: list = ["PUBLISH", channel, message]

        return await self.run(command=command)

    async def pubsub(self) -> "PubSub":
        """
        See https://redis.io/commands/pubsub
        """

        return PubSub(client=self)

    async def eval(self, script: str, keys: list[str] = None, arguments: list[str] = None) -> Any:
        """
        See https://redis.io/commands/eval

        The keys and arguments can be specified with the same-name parameters.
        The number of keys is calculated automatically.
        """

        command: list = ["EVAL", script]

        if keys:
            command.extend([len(keys), *keys])

        if arguments:
            command.extend(arguments)

        return await self.run(command=command)

    async def evalsha(
        self,
        sha1_digest: str,
        keys: list = None,
        arguments: list = None
    ) -> Any:
        """
        See https://redis.io/commands/evalsha

        The keys and arguments can be specified with the same-name parameters.
        The number of keys is calculated automatically.
        """

        command: list = ["EVALSHA", sha1_digest]

        if keys:
            command.extend([len(keys), *keys])

        if arguments:
            command.extend(arguments)

        return await self.run(command=command)

    async def script(self) -> "Script":
        """
        See https://redis.io/commands/script
        """

        return Script(client=self)

    """
    Need to double-check compatibility with the classic Redis API for this one.
    async def acl(self) -> "ACL":
        # See https://redis.io/commands/acl
        
        return ACL(client=self)
    """

    async def dbsize(self) -> int:
        """
        See https://redis.io/commands/dbsize
        """

        command: list = ["DBSIZE"]

        return await self.run(command=command)

    async def flushall(self, mode: Literal["ASYNC", "SYNC"] = None) -> str:
        """
        See https://redis.io/commands/flushall

        The mode(ASYNC/SYNC) can be specified with the same-name parameter.
        """

        command: list = ["FLUSHALL"]

        if mode:
            command.append(mode)

        return await self.run(command=command)

    async def flushdb(self, mode: Literal["ASYNC", "SYNC"] = None) -> str:
        """
        See https://redis.io/commands/flushdb

        The mode(ASYNC/SYNC) can be specified with the same-name parameter.
        """

        command: list = ["FLUSHDB"]

        if mode:
            command.append(mode)

        return await self.run(command=command)

    async def server_time(self) -> list[str] | dict[str, int]:
        """
        See https://redis.io/commands/time
        """

        command: list = ["TIME"]

        raw: list[str] = await self.run(command=command)

        return format_time_output(raw=raw) if self.format_return else raw

    async def sadd(self, key: str, *members: Any) -> int:
        """
        See https://redis.io/commands/sadd
        """

        command: list = ["SADD", key, *members]

        return await self.run(command=command)

    async def scard(self, key: str) -> int:
        """
        See https://redis.io/commands/scard
        """

        command: list = ["SCARD", key]

        return await self.run(command=command)

    async def sdiff(self, *keys: str) -> list[str]:
        """
        See https://redis.io/commands/sdiff
        """

        command: list = ["SDIFF", *keys]

        return await self.run(command=command)

    async def sdiffstore(self, destination_key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/sdiffstore
        """

        command: list = ["SDIFFSTORE", destination_key, *keys]

        return await self.run(command=command)

    async def sinter(self, *keys: str) -> list[str]:
        """
        See https://redis.io/commands/sinter
        """

        command: list = ["SINTER", *keys]

        return await self.run(command=command)

    async def sinterstore(self, destination_key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/sinterstore
        """

        command: list = ["SINTERSTORE", destination_key, *keys]

        return await self.run(command=command)

    async def sismember(self, key: str, member: Any) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/sismember
        """

        command: list = ["SISMEMBER", key, member]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def smembers(self, key: str) -> list[str]:
        """
        See https://redis.io/commands/smembers
        """

        command: list = ["SMEMBERS", key]

        return await self.run(command=command)

    async def smove(self, source_key: str, destination_key: str, member: Any) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/smove
        """

        command: list = ["SMOVE", source_key, destination_key, member]

        raw: Literal[1, 0] = await self.run(command=command)

        return bool(raw) if self.format_return else raw

    async def spop(self, key: str, count: int | None = None) -> (str | None) | list[str]:
        """
        See https://redis.io/commands/spop
        """

        command: list = ["SPOP", key]

        if count:
            command.append(count)

        return await self.run(command=command)

    async def srandmember(self, key: str, count: int | None = None) -> (str | None) | list[str]:
        """
        See https://redis.io/commands/srandmember
        """

        command: list = ["SRANDMEMBER", key]

        if count:
            command.append(count)

        return await self.run(command=command)

    async def srem(self, key: str, *members: Any) -> int:
        """
        See https://redis.io/commands/srem
        """

        command: list = ["SREM", key, *members]

        return await self.run(command=command)

    async def sscan(
        self,
        key: str,
        cursor: int,
        pattern: str | None = None,
        count: int | None = None,
        return_cursor: bool = True,
    ) -> list[int | list[str]] | list[str]:
        """
        See https://redis.io/commands/sscan

        "MATCH" was replaced with "pattern".

        If "return_cursor" is False, it won't return the cursor.
        """

        command: list = ["SSCAN", key, cursor]

        if pattern:
            command.extend(["MATCH", pattern])

        if count:
            command.extend(["COUNT", count])

        raw: list[int | list[str]] = await self.run(command=command)

        # The raw result is composed of the new cursor and the array of elements.
        return raw[1] if not return_cursor else raw

    async def sunion(self, *keys: str) -> list[str]:
        """
        See https://redis.io/commands/sunion
        """

        command: list = ["SUNION", *keys]

        return await self.run(command=command)

    async def sunionstore(self, destination_key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/sunionstore
        """

        command: list = ["SUNIONSTORE", destination_key, *keys]

        return await self.run(command=command)

    async def zadd(
        self,
        key: str,
        sorted_set_members: dict,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
        incr: bool = False,
    ) -> int | (str | None | float):
        """
        See https://redis.io/commands/zadd

        The members should be added with a dict containing their names and scores.
        """

        if nx and xx:
            raise Exception(
                """
                "NX" and "XX" are mutually exclusive.
                """
            )

        if gt and lt:
            raise Exception(
                """
                "GT" and "LT" are mutually exclusive.
                """
            )

        if nx and (gt or lt):
            raise Exception(
                """
                "NX" and "GT"/"LT" are mutually exclusive.
                """
            )

        command: list = ["ZADD", key]

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

            for name, score in sorted_set_members.items():
                command.extend([score, name])

            raw: (str | None) = await self.run(command=command)

            return float(raw) if self.format_return and raw else raw

        for name, score in sorted_set_members.items():
            command.extend([score, name])

        return await self.run(command=command)

    async def zcard(self, key: str) -> int:
        """
        See https://redis.io/commands/zcard
        """

        command: list = ["ZCARD", key]

        return await self.run(command=command)

    async def zcount(self, key: str, min_score: int | str, max_score: int | str) -> int:
        """
        See https://redis.io/commands/zcount

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: list = ["ZCOUNT", key, min_score, max_score]

        return await self.run(command=command)

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is specified or not, its return type will be list[str].
    """
    async def zdiff(self, *keys: str, with_scores: bool = False) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zdiff

        The number of keys is calculated automatically.
        """

        command: list = ["ZDIFF", len(keys), *keys]

        if with_scores:
            command.append("WITHSCORES")

        raw: SortedSetReturn = await self.run(command=command)

        return format_sorted_set(raw) if self.format_return and with_scores else raw

    async def zdiffstore(self, destination_key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/zdiffstore

        The number of keys is calculated automatically.
        """

        command: list = ["ZDIFFSTORE", destination_key, len(keys), *keys]

        return await self.run(command=command)

    async def zincrby(self, key: str, increment: float, member: str) -> str | float:
        """
        See https://redis.io/commands/zincrby
        """

        command: list = ["ZINCRBY", key, increment, member]

        raw: str = await self.run(command=command)

        return float(raw) if self.format_return else raw

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is specified or not, its return type will be list[str].
    """
    async def zinter(
        self,
        *keys: str,
        multiplication_factors: list[float] | None = None,
        aggregate: Literal["SUM", "MIN", "MAX"] | None = None,
        with_scores: bool = False,
    ) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zinter

        The number of keys is calculated automatically.

        The "WEIGHTS" can be specified with "multiplication_factors".
        """

        command: list = ["ZINTER", len(keys), *keys]

        if multiplication_factors:
            command.extend(["WEIGHTS", *multiplication_factors])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        if with_scores:
            command.append("WITHSCORES")

        raw: SortedSetReturn = await self.run(command=command)

        return format_sorted_set(raw) if self.format_return and with_scores else raw

    async def zinterstore(
        self,
        destination_key: str,
        *keys: str,
        multiplication_factors: list[float] | None = None,
        aggregate: Literal["SUM", "MIN", "MAX"] | None = None,
    ) -> int:
        """
        See https://redis.io/commands/zinterstore

        The number of keys is calculated automatically.

        The "WEIGHTS" can be specified with "multiplication_factors".
        """

        command: list = ["ZINTERSTORE", destination_key, len(keys), *keys]

        if multiplication_factors:
            command.extend(["WEIGHTS", *multiplication_factors])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        return await self.run(command=command)

    async def get(self, key: str) -> str:
        """
        See https://redis.io/commands/get
        """

        command: list = ["GET", key]

        return await self.run(command=command)

    async def set(self, key: str, value: Any) -> str:
        """
        See https://redis.io/commands/set
        """

        command: list = ["SET", key, value]

        return await self.run(command=command)


# It doesn't inherit from "Redis" mainly because of the methods signatures.
class BitFieldCommands:
    def __init__(self, client: Redis, key: str):
        self.client = client
        self.command: list = ["BITFIELD", key]

    def get(self, encoding: str, offset: BitFieldOffset) -> Self:
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    def set(self, encoding: str, offset: BitFieldOffset, value: int) -> Self:
        """
        Set the specified bit field and returns its old value.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["SET", encoding, offset, value]
        self.command.extend(_command)

        return self

    def incrby(self, encoding: str, offset: BitFieldOffset, increment: int) -> Self:
        """
        Increments or decrements (if a negative increment is given) the specified bit field and returns the new value.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["INCRBY", encoding, offset, increment]
        self.command.extend(_command)

        return self

    def overflow(self, overflow: Literal["WRAP", "SAT", "FAIL"]) -> Self:
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

    async def execute(self) -> RESTResult:
        return await self.client.run(command=self.command)


class BitFieldRO:
    def __init__(self, client: Redis, key: str):
        self.client = client
        self.command: list = ["BITFIELD_RO", key]

    def get(self, encoding: str, offset: BitFieldOffset) -> Self:
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    async def execute(self) -> RESTResult:
        return await self.client.run(command=self.command)


class PubSub:
    def __init__(self, client: Redis):
        self.client = client
        self.command: list = ["PUBSUB"]

    async def channels(self, pattern: str = None) -> list[str]:
        """
        See https://redis.io/commands/pubsub-channels
        """

        self.command.append("CHANNELS")

        if pattern:
            self.command.append(pattern)

        return await self.client.run(command=self.command)

    async def numpat(self) -> int:
        """
        See https://redis.io/commands/pubsub-numpat
        """

        self.command.append("NUMPAT")

        return await self.client.run(command=self.command)

    async def numsub(self, *channels: str) -> list[str | int] | dict[str, int]:
        """
        See https://redis.io/commands/pubsub-numsub
        """

        self.command.extend(["NUMSUB", *channels])

        raw: list[str | int] = await self.client.run(command=self.command)

        return format_pubsub_numsub(raw=raw) if self.client.format_return else raw


class Script:
    def __init__(self, client: Redis):
        self.client = client
        self.command: list = ["SCRIPT"]

    async def exists(self, *sha1_digests: str) -> list[Literal[1, 0]] | list[bool]:
        """
        See https://redis.io/commands/script-exists
        """

        self.command.extend(["EXISTS", *sha1_digests])

        raw: list[Literal[1, 0]] = await self.client.run(command=self.command)

        return format_bool_list(raw=raw) if self.client.format_return else raw

    async def flush(self, mode: Literal["ASYNC", "SYNC"]) -> str:
        """
        See https://redis.io/commands/script-flush

        The mode(ASYNC/SYNC) can be specified with the same-name parameter.
        """

        self.command.append("FLUSH")

        if mode:
            self.command.append(mode)

        return await self.client.run(command=self.command)

    async def load(self, script: str) -> str:
        """
        See https://redis.io/commands/script-load
        """

        self.command.extend(["LOAD", script])

        return await self.client.run(command=self.command)


"""
class ACL:
    def __init__(self, client: Redis):
        self.client = client
        self.command: list = ["ACL"]

    async def cat(self, category_name: str | None = None) -> list[str]:
        # See https://redis.io/commands/acl-cat

        self.command.append("CAT")

        if category_name:
            self.command.append(category_name)

        return await self.client.run(command=self.command)

    async def deluser(self, *usernames: str) -> int:
        # See https://redis.io/commands/acl-deluser

        self.command.extend(["DELUSER", *usernames])

        return await self.client.run(command=self.command)

    async def genpass(self, bits: int = None) -> str:
        # See https://redis.io/commands/acl-genpass

        self.command.append("GENPASS")

        if bits:
            self.command.append(bits)

        return await self.client.run(command=self.command)

    # Is it possible to format this output?
    async def getuser(self, username: str) -> list[str] | None:
        # See https://redis.io/commands/acl-getuser

        self.command.extend(["GETUSER", username])

        return await self.client.run(command=self.command)

    async def list_rules(self) -> list[str]:
        # See https://redis.io/commands/acl-list

        self.command.append("LIST")

        return await self.client.run(command=self.command)

    async def load(self) -> str:
        # See https://redis.io/commands/acl-load

        self.command.append("LOAD")

        return await self.client.run(command=self.command)

    async def log(self, count: int = None, reset: bool = False) -> list[str]:
        # See https://redis.io/commands/acl-log

        if count is not None and reset:
            raise Exception("Cannot specify both count and reset.")

        self.command.append("LOG")

        if count:
            self.command.append(count)

        if reset:
            self.command.append("RESET")

        return await self.client.run(command=self.command)
"""