from upstash_redis.http.execute import execute
from upstash_redis.schema.http import RESTResult, RESTEncoding
from upstash_redis.schema.telemetry import TelemetryData
from upstash_redis.config import (
    REST_ENCODING,
    REST_RETRIES,
    REST_RETRY_INTERVAL,
    ALLOW_TELEMETRY,
    FORMAT_RETURN
)
from upstash_redis.utils.format import (
    format_geo_positions_return,
    format_geo_members_return,
    format_hash_return,
    format_pubsub_numsub_return,
    format_bool_list,
    format_server_time_return,
    format_sorted_set_return,
    format_float_list
)
from upstash_redis.utils.exception import (
    handle_geosearch_exceptions,
    handle_non_deprecated_zrange_exceptions,
    handle_zrangebylex_exceptions,
    handle_georadius_write_exceptions
)
from upstash_redis.utils.comparison import number_are_not_none
from upstash_redis.schema.commands.parameters import BitFieldOffset, GeoMember, FloatMinMax
from upstash_redis.schema.commands.returns import (
    GeoMembersReturn,
    FormattedGeoMembersReturn,
    HashReturn,
    FormattedHashReturn,
    SortedSetReturn,
    FormattedSortedSetReturn
)
from aiohttp import ClientSession
from typing import Type, Any, Literal
from os import environ


class Redis:
    def __init__(
        self,
        url: str,
        token: str,
        rest_encoding: RESTEncoding = REST_ENCODING,
        rest_retries: int = REST_RETRIES,
        rest_retry_interval: int = REST_RETRY_INTERVAL,  # Seconds.
        format_return: bool = FORMAT_RETURN,
        allow_telemetry: bool = ALLOW_TELEMETRY,
        telemetry_data: TelemetryData | None = None
    ):
        """
        :param url: UPSTASH_REDIS_REST_URL in the console
        :param token: UPSTASH_REDIS_REST_TOKEN in the console
        :param rest_encoding: the encoding that can be used by the REST API to parse the response before sending it
        :param rest_retries: how many times an HTTP request will be retried if it fails
        :param rest_retry_interval: how many seconds will be waited between each retry
        :param format_return: whether the raw, RESP2 result or a formatted response will be returned
        :param allow_telemetry: whether anonymous telemetry can be collected
        """

        self.url = url
        self.token = token

        self.allow_telemetry = allow_telemetry

        self.format_return = format_return

        self.rest_encoding = rest_encoding
        self.rest_retries = rest_retries
        self.rest_retry_interval = rest_retry_interval

        self.telemetry_data = telemetry_data

    @classmethod
    def from_env(
        cls,
        rest_encoding: RESTEncoding = REST_ENCODING,
        rest_retries: int = REST_RETRIES,
        rest_retry_interval: int = REST_RETRY_INTERVAL,
        format_return: bool = FORMAT_RETURN,
        allow_telemetry: bool = ALLOW_TELEMETRY,
        telemetry_data: TelemetryData | None = None
    ):
        """
        Load the credentials from environment.

        :param rest_encoding: the encoding that can be used by the REST API to parse the response before sending it
        :param rest_retries: how many times an HTTP request will be retried if it fails
        :param rest_retry_interval: how many seconds will be waited between each retry
        :param format_return: whether the raw, RESP2 result or a formatted response will be returned
        :param allow_telemetry: whether anonymous telemetry can be collected
        """

        return cls(
            environ["UPSTASH_REDIS_REST_URL"],
            environ["UPSTASH_REDIS_REST_TOKEN"],
            rest_encoding,
            rest_retries,
            rest_retry_interval,
            format_return,
            allow_telemetry,
            telemetry_data
        )

    async def __aenter__(self) -> ClientSession:
        """
        Enter the async context.
        """

        self._session: ClientSession = ClientSession()
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
            allow_telemetry=self.allow_telemetry,
            telemetry_data=self.telemetry_data
        )

    async def bitcount(self, key: str, start: int | None = None, end: int | None = None) -> int:
        """
        See https://redis.io/commands/bitcount
        """

        if number_are_not_none(start, end, number=1):
            raise Exception("Both \"start\" and \"end\" must be specified.")

        command: list = ["BITCOUNT", key]

        if start is not None:
            command.extend([start, end])

        return await self.run(command)

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

        if len(source_keys) == 0:
            raise Exception("At least one source key must be specified.")

        if operation == "NOT" and len(source_keys) > 1:
            raise Exception("The \"NOT \" operation takes only one source key as argument.")

        command: list = ["BITOP", operation, destination_key, *source_keys]

        return await self.run(command)

    async def bitpos(self, key: str, bit: Literal[0, 1], start: int | None = None, end: int | None = None) -> int:
        """
        See https://redis.io/commands/bitpos
        """

        if start is None and end is not None:
            raise Exception("\"end\" is specified, but \"start\" is missing.")

        command: list = ["BITPOS", key, bit]

        if start is not None:
            command.append(start)

        if end is not None:
            command.append(end)

        return await self.run(command)

    async def getbit(self, key: str, offset: int) -> int:
        """
        See https://redis.io/commands/getbit
        """

        command: list = ["GETBIT", key, offset]

        return await self.run(command)

    async def setbit(self, key: str, offset: int, value: Literal[0, 1]) -> int:
        """
        See https://redis.io/commands/setbit
        """

        command: list = ["SETBIT", key, offset, value]

        return await self.run(command)

    async def ping(self, message: str | None = None) -> str:
        """
        See https://redis.io/commands/ping
        """

        command: list = ["PING"]

        if message is not None:
            command.append(message)

        return await self.run(command)

    async def echo(self, message: str) -> str:
        """
        See https://redis.io/commands/echo
        """

        command: list = ["ECHO", message]

        return await self.run(command)

    async def copy(self, source: str, destination: str, replace: bool = False) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/copy

        :return: A bool if "format_return" is True.
        """

        command: list = ["COPY", source, destination]

        if replace:
            command.append("REPLACE")

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def delete(self, *keys: str) -> int:
        """
        See https://redis.io/commands/del
        """

        if len(keys) == 0:
            raise Exception("At least one key must be deleted.")

        command: list = ["DEL", *keys]

        return await self.run(command)

    async def exists(self, *keys: str) -> int:
        """
        See https://redis.io/commands/exists
        """

        if len(keys) == 0:
            raise Exception("At least one key must be checked.")

        command: list = ["EXISTS", *keys]

        return await self.run(command)

    async def expire(self, key: str, seconds: int) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/expire

        :return: A bool if "format_return" is True.
        """

        command: list = ["EXPIRE", key, seconds]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def expireat(self, key: str, unix_time_seconds: int) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/expireat

        :return: A bool if "format_return" is True.
        """

        command: list = ["EXPIREAT", key, unix_time_seconds]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def keys(self, pattern: str) -> list[str]:
        """
        See https://redis.io/commands/keys
        """

        command: list = ["KEYS", pattern]

        return await self.run(command)

    async def persist(self, key: str) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/persist

        :return: A bool if "format_return" is True.
        """

        command: list = ["PERSIST", key]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def pexpire(self, key: str, milliseconds: int) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/pexpire

        :return: A bool if "format_return" is True.
        """

        command: list = ["PEXPIRE", key, milliseconds]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def pexpireat(self, key: str, unix_time_milliseconds: int) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/pexpireat

        :return: A bool if "format_return" is True.
        """

        command: list = ["PEXPIREAT", key, unix_time_milliseconds]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def pttl(self, key: str) -> int:
        """
        See https://redis.io/commands/pttl
        """

        command: list = ["PTTL", key]

        return await self.run(command)

    async def randomkey(self) -> str | None:
        """
        See https://redis.io/commands/randomkey
        """

        command: list = ["RANDOMKEY"]

        return await self.run(command)

    async def rename(self, key: str, new_name: str) -> str:
        """
        See https://redis.io/commands/rename
        """

        command: list = ["RENAME", key, new_name]

        return await self.run(command)

    async def renamenx(self, key: str, new_name: str) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/renamenx

        :return: A bool if "format_return" is True.
        """

        command: list = ["RENAMENX", key, new_name]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def scan(
        self,
        cursor: int,
        pattern: str | None = None,
        count: int | None = None,
        scan_type: str | None = None,
        return_cursor: bool = True
    ) -> (list[str | list[str]] | list[int | list[str]]) | list[str]:
        """
        See https://redis.io/commands/scan

        :param pattern: replacement for "MATCH"

        :param count: defaults to 10 on the server side

        :param scan_type: replacement for "TYPE"

        :param return_cursor: if set to False, it won't return the cursor

        :return: The cursor will be an integer if "format_return" is True.
        Only the list of elements will be returned if "return_cursor" is False
        """

        command: list = ["SCAN", cursor]

        if pattern is not None:
            command.extend(["MATCH", pattern])

        if count is not None:
            command.extend(["COUNT", count])

        if scan_type is not None:
            command.extend(["TYPE", scan_type])

        # The raw result is composed of the new cursor and the list of elements.
        raw: list[str | list[str]] = await self.run(command)

        if return_cursor:
            return [int(raw[0]), raw[1]] if self.format_return else raw

        return raw[1]

    async def touch(self, *keys: str) -> int:
        """
        See https://redis.io/commands/touch
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["TOUCH", *keys]

        return await self.run(command)

    async def ttl(self, key: str) -> int:
        """
        See https://redis.io/commands/ttl
        """

        command: list = ["TTL", key]

        return await self.run(command)

    async def type(self, key: str) -> str | None:
        """
        See https://redis.io/commands/type
        """

        command: list = ["TYPE", key]

        return await self.run(command)

    async def unlink(self, *keys: str) -> int:
        """
        See https://redis.io/commands/unlink
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["UNLINK", *keys]

        return await self.run(command)

    async def geoadd(
        self,
        key: str,
        *members: GeoMember,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> int:
        """
        See https://redis.io/commands/geoadd

        :param members: a sequence of GeoMember dict types (longitude, latitude, name).
        """

        if len(members) == 0:
            raise Exception("At least one member must be added.")

        if nx and xx:
            raise Exception("\"nx\" and \"xx\" are mutually exclusive.")

        command: list = ["GEOADD", key]

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        if ch:
            command.append("CH")

        for member in members:
            command.extend([member["longitude"], member["latitude"], member["name"]])

        return await self.run(command)

    async def geodist(
        self,
        key: str,
        first_member: str,
        second_member: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"] = "M"
    ) -> str | float | None:
        """
        See https://redis.io/commands/geodist

        :return: A float value if "format_return" is True.
        """

        command: list = ["GEODIST", key, first_member, second_member, unit]

        raw: str | None = await self.run(command)

        return float(raw) if self.format_return else raw

    async def geohash(self, key: str, *members: str) -> list[str | None]:
        """
        See https://redis.io/commands/geohash
        """

        command: list = ["GEOHASH", key, *members]

        return await self.run(command)

    async def geopos(
        self,
        key: str,
        *members: str
    ) -> list[list[str] | None] | list[dict[str, float] | None]:
        """
        See https://redis.io/commands/geopos

        :return: A list of dicts with either None or the longitude and latitude if "format_return" is True.
        """

        command: list = ["GEOPOS", key, *members]

        raw: list[list[str] | None] = await self.run(command)

        return format_geo_positions_return(raw) if self.format_return else raw

    async def georadius(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        with_distance: bool = False,
        with_hash: bool = False,
        with_coordinates: bool = False,
        count: int | None = None,
        count_any: bool = False,
        sort: Literal["ASC", "DESC"] | None = None,
        store_as: str | None = None,
        store_distance_as: str | None = None
    ) -> GeoMembersReturn | FormattedGeoMembersReturn | int:
        """
        See https://redis.io/commands/georadius

        :param count_any: replacement for "ANY"

        :param store_as: replacement for "STORE"
        :param store_distance_as: replacement for "STORE_DIST"

        :return: A list of dicts with the requested properties if "format_return" is True.
        """

        handle_georadius_write_exceptions(
            with_distance,
            with_hash,
            with_coordinates,
            count,
            count_any,
            store_as,
            store_distance_as
        )

        command: list = ["GEORADIUS", key, longitude, latitude, radius, unit]

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        if store_as:
            command.extend(["STORE", store_as])

        if store_distance_as:
            command.extend(["STOREDIST", store_distance_as])

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "list[str]".
        if self.format_return and (with_distance or with_hash or with_coordinates):
            return format_geo_members_return(raw, with_distance, with_hash, with_coordinates)

        return raw

    async def georadius_ro(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        with_distance: bool = False,
        with_hash: bool = False,
        with_coordinates: bool = False,
        count: int | None = None,
        count_any: bool = False,
        sort: Literal["ASC", "DESC"] | None = None
    ) -> GeoMembersReturn | FormattedGeoMembersReturn:
        """
        See https://redis.io/commands/georadius_ro

        :param count_any: replacement for "ANY"

        :return: A list of dicts with the requested properties if "format_return" is True.
        """

        if count_any and count is None:
            raise Exception("\"count_any\" can only be used together with \"count\".")

        command: list = ["GEORADIUS_RO", key, longitude, latitude, radius, unit]

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "list[str]".
        if self.format_return and (with_distance or with_hash or with_coordinates):
            return format_geo_members_return(raw, with_distance, with_hash, with_coordinates)

        return raw

    async def georadiusbymember(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
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

        :param count_any: replacement for "ANY"

        :param store_as: replacement for "STORE"
        :param store_distance_as: replacement for "STORE_DIST"

        :return: A list of dicts with the requested properties if "format_return" is True.
        """

        handle_georadius_write_exceptions(
            with_distance,
            with_hash,
            with_coordinates,
            count,
            count_any,
            store_as,
            store_distance_as
        )

        command: list = ["GEORADIUSBYMEMBER", key, member, radius, unit]

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        if store_as:
            command.extend(["STORE", store_as])

        if store_distance_as:
            command.extend(["STOREDIST", store_distance_as])

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "list[str]".
        if self.format_return and (with_distance or with_hash or with_coordinates):
            return format_geo_members_return(raw, with_distance, with_hash, with_coordinates)

        return raw

    async def georadiusbymember_ro(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        with_distance: bool = False,
        with_hash: bool = False,
        with_coordinates: bool = False,
        count: int | None = None,
        count_any: bool = False,
        sort: Literal["ASC", "DESC"] | None = None
    ) -> GeoMembersReturn | FormattedGeoMembersReturn:
        """
        See https://redis.io/commands/georadiusbymember

        :param count_any: replacement for "ANY"

        :return: A list of dicts with the requested properties if "format_return" is True.
        """

        if count_any and count is None:
            raise Exception("\"count_any\" can only be used together with \"count\".")

        command: list = ["GEORADIUSBYMEMBER_RO", key, member, radius, unit]

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if sort:
            command.append(sort)

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "list[str]".
        if self.format_return and (with_distance or with_hash or with_coordinates):
            return format_geo_members_return(raw, with_distance, with_hash, with_coordinates)

        return raw

    async def geosearch(
        self,
        key: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        member: str | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: Literal["ASC", "DESC"] | None = None,
        count: int | None = None,
        count_any: bool = False,
        with_distance: bool = False,
        with_hash: bool = False,
        with_coordinates: bool = False
    ) -> GeoMembersReturn | FormattedGeoMembersReturn:
        """
        See https://redis.io/commands/geosearch

        :param member: replacement for "FROMMEMBER"

        :param longitude: replacement for "FROMLONLAT" together with "latitude"
        :param latitude: replacement for "FROMLONLAT" together with "longitude"

        :param radius: replacement for "BYRADIUS"

        :param width: replacement for "BYBOX" together with "height"
        :param height: replacement for "BYBOX" together with "width"

        :param count_any: replacement for "ANY"

        :return: A list of dicts with the requested properties if "format_return" is True.
        """

        handle_geosearch_exceptions(member, longitude, latitude, radius, width, height, count, count_any)

        command: list = ["GEOSEARCH", key]

        if member is not None:
            command.extend(["FROMMEMBER", member])

        if longitude is not None:
            command.extend(["FROMLONLAT", longitude, latitude])

        if radius is not None:
            command.extend(["BYRADIUS", radius])

        if width is not None:
            command.extend(["BYBOX", width, height])

        command.append(unit)

        if sort:
            command.append(sort)

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

        if with_coordinates:
            command.append("WITHCOORD")

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "list[str]".
        if self.format_return and (with_distance or with_hash or with_coordinates):
            return format_geo_members_return(raw, with_distance, with_hash, with_coordinates)

        return raw

    async def geosearchstore(
        self,
        destination_key: str,
        source_key: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        member: str | None = None,
        longitude: float | None = None,
        latitude: float | None = None,
        radius: float | None = None,
        width: float | None = None,
        height: float | None = None,
        sort: Literal["ASC", "DESC"] | None = None,
        count: int | None = None,
        count_any: bool = False,
        store_distance: bool = False
    ) -> int:
        """
        See https://redis.io/commands/geosearchstore

        :param member: replacement for "FROMMEMBER"

        :param longitude: replacement for "FROMLONLAT" together with "latitude"
        :param latitude: replacement for "FROMLONLAT" together with "longitude"

        :param radius: replacement for "BYRADIUS"

        :param width: replacement for "BYBOX" together with "height"
        :param height: replacement for "BYBOX" together with "width"

        :param count_any: replacement for "ANY"

        :param store_distance: replacement for "STOREDIST"
        """

        handle_geosearch_exceptions(member, longitude, latitude, radius, width, height, count, count_any)

        command: list = ["GEOSEARCHSTORE", destination_key, source_key]

        if member is not None:
            command.extend(["FROMMEMBER", member])

        if longitude is not None:
            command.extend(["FROMLONLAT", longitude, latitude])

        if radius is not None:
            command.extend(["BYRADIUS", radius])

        if width is not None:
            command.extend(["BYBOX", width, height])

        command.append(unit)

        if sort:
            command.append(sort)

        if count is not None:
            command.extend(["COUNT", count])
            if count_any:
                command.append("ANY")

        if store_distance:
            command.append("STOREDIST")

        return await self.run(command)

    async def hdel(self, key: str, *fields: str) -> int:
        """
        See https://redis.io/commands/hdel
        """

        if len(fields) == 0:
            raise Exception("At least one field must be deleted.")

        command: list = ["HDEL", key, *fields]

        return await self.run(command)

    async def hexists(self, key: str, field: str) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/hexists

        :return: A bool if "format_return" is True.
        """

        command: list = ["HEXISTS", key, field]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def hget(self, key: str, field: str) -> str | None:
        """
        See https://redis.io/commands/hget
        """

        command: list = ["HGET", key, field]

        return await self.run(command)

    async def hgetall(self, key: str) -> HashReturn | FormattedHashReturn:
        """
        See https://redis.io/commands/hgetall

        :return: A dict of field-value pairs if "format_return" is True.
        """

        command: list = ["HGETALL", key]

        raw: HashReturn = await self.run(command)

        return format_hash_return(raw) if self.format_return else raw

    async def hincrby(self, key: str, field: str, increment: int) -> int:
        """
        See https://redis.io/commands/hincrby
        """

        command: list = ["HINCRBY", key, field, increment]

        return await self.run(command)

    async def hincrbyfloat(self, key: str, field: str, increment: float) -> str | float:
        """
        See https://redis.io/commands/hincrbyfloat

        :return: A float if "format_return" is True.
        """

        command: list = ["HINCRBYFLOAT", key, field, increment]

        raw: str = await self.run(command)

        return float(raw) if self.format_return else raw

    async def hkeys(self, key: str) -> list[str]:
        """
        See https://redis.io/commands/hkeys
        """

        command: list = ["HKEYS", key]

        return await self.run(command)

    async def hlen(self, key: str) -> int:
        """
        See https://redis.io/commands/hlen
        """

        command: list = ["HLEN", key]

        return await self.run(command)

    async def hmget(self, key: str, *fields: str) -> list[str | None]:
        """
        See https://redis.io/commands/hmget
        """

        if len(fields) == 0:
            raise Exception("At least one field must be specified.")

        command: list = ["HMGET", key, *fields]

        return await self.run(command)

    async def hmset(self, key: str, fields_and_values: dict) -> str:
        """
        See https://redis.io/commands/hmset
        """

        command: list = ["HMSET", key]

        for field, value in fields_and_values.items():
            command.extend([field, value])

        return await self.run(command)

    async def hrandfield(
        self,
        key: str,
        count: int | None = None,
        with_values: bool = False
    ) -> (str | None) | (HashReturn | FormattedHashReturn):
        """
        See https://redis.io/commands/hrandfield

        :param count: defaults to 1 on the server side

        :return: A dict of field-value pairs if "count" and "with_values" are specified and "format_return" is True.
        """

        if count is None and with_values:
            raise Exception("\"with_values\" can only be used together with \"count\"")

        command: list = ["HRANDFIELD", key]

        if count is not None:
            command.extend(["COUNT", count])

            if with_values:
                command.append("WITHVALUES")

                raw: HashReturn = await self.run(command)

                return format_hash_return(raw) if self.format_return else raw

        return await self.run(command)

    async def hscan(
        self,
        key: str,
        cursor: int,
        pattern: str | None = None,
        count: int | None = None,
        return_cursor: bool = True
    ) -> (list[str | HashReturn] | list[int | FormattedHashReturn]) | (HashReturn | FormattedHashReturn):
        """
        See https://redis.io/commands/hscan

        :param pattern: replacement for "MATCH"

        :param count: defaults to 10 on the server side

        :param return_cursor: if set to False, it won't return the cursor

        :return: The cursor will be an integer if "format_return" is True.
        Only a dict of field-value pairs will be returned if "return_cursor" is False and "format_return" is True.
        """

        command: list = ["HSCAN", key, cursor]

        if pattern is not None:
            command.extend(["MATCH", pattern])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the list of elements.
        raw: list[str | HashReturn] | HashReturn = await self.run(command)

        if return_cursor:
            return [int(raw[0]), format_hash_return(raw[1])] if self.format_return else raw

        return format_hash_return(raw[1]) if self.format_return else raw[1]

    async def hset(self, key: str, fields_and_values: dict) -> int:
        """
        See https://redis.io/commands/hset
        """

        command: list = ["HSET", key]

        for field, value in fields_and_values.items():
            command.extend([field, value])

        return await self.run(command)

    async def hsetnx(self, key: str, field: str, value: Any) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/hsetnx

        :return: A bool if "format_return" is True.
        """

        command: list = ["HSETNX", key, field, value]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def hstrlen(self, key: str, field: str) -> int:
        """
        See https://redis.io/commands/hstrlen
        """

        command: list = ["HSTRLEN", key, field]

        return await self.run(command)

    async def hvals(self, key: str) -> list[str]:
        """
        See https://redis.io/commands/hvals
        """

        command: list = ["HVALS", key]

        return await self.run(command)

    async def pfadd(self, key: str, *elements: Any) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/pfadd

        :return: A bool if "format_return" is True.
        """

        command: list = ["PFADD", key, *elements]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def pfcount(self, *keys: str) -> int:
        """
        See https://redis.io/commands/pfcount
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["PFCOUNT", *keys]

        return await self.run(command)

    async def pfmerge(self, destination_key: str, *source_keys: str) -> str:
        """
        See https://redis.io/commands/pfmerge
        """

        command: list = ["PFMERGE", destination_key, *source_keys]

        return await self.run(command)

    async def lindex(self, key: str, index: int) -> str | None:
        """
        See https://redis.io/commands/lindex
        """

        command: list = ["LINDEX", key, index]

        return await self.run(command)

    async def linsert(
        self,
        key: str,
        position: Literal["BEFORE", "AFTER"],
        pivot: Any,
        element: Any
    ) -> int:
        """
        See https://redis.io/commands/linsert
        """

        command: list = ["LINSERT", key, position, pivot, element]

        return await self.run(command)

    async def llen(self, key: str) -> int:
        """
        See https://redis.io/commands/llen
        """

        command: list = ["LLEN", key]

        return await self.run(command)

    async def lmove(
        self,
        source_key: str,
        destination_key: str,
        source_position: Literal["LEFT", "RIGHT"],
        destination_position: Literal["LEFT", "RIGHT"]
    ) -> str | None:
        """
        See https://redis.io/commands/lmove
        """

        command: list = ["LMOVE", source_key, destination_key, source_position, destination_position]

        return await self.run(command)

    async def lpop(self, key: str, count: int | None = None) -> (str | None) | list[str]:
        """
        See https://redis.io/commands/lpop

        :param count: defaults to 1 on the server side
        """

        command: list = ["LPOP", key]

        if count is not None:
            command.append(count)

        return await self.run(command)

    async def lpos(
        self,
        key: str,
        element: Any,
        first_return: int | None = None,
        count: int | None = None,
        max_number_of_comparisons: int | None = None
    ) -> (int | None) | list[int]:
        """
        See https://redis.io/commands/lpos

        :param first_return: replacement for "RANK"

        :param max_number_of_comparisons: replacement for "MAXLEN"
        """

        command: list = ["LPOS", key, element]

        if first_return is not None:
            command.extend(["RANK", first_return])

        if count is not None:
            command.extend(["COUNT", count])

        if max_number_of_comparisons is not None:
            command.extend(["MAXLEN", max_number_of_comparisons])

        return await self.run(command)

    async def lpush(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/lpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: list = ["LPUSH", key, *elements]

        return await self.run(command)

    async def lpushx(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/lpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: list = ["LPUSHX", key, *elements]

        return await self.run(command)

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        """
        See https://redis.io/commands/lrange
        """

        command: list = ["LRANGE", key, start, stop]

        return await self.run(command)

    async def lrem(self, key: str, count: int, element: Any) -> int:
        """
        See https://redis.io/commands/lrem
        """

        command: list = ["LREM", key, count, element]

        return await self.run(command)

    async def lset(self, key: str, index: int, element: Any) -> str:
        """
        See https://redis.io/commands/lset
        """

        command: list = ["LSET", key, index, element]

        return await self.run(command)

    async def ltrim(self, key: str, start: int, stop: int) -> str:
        """
        See https://redis.io/commands/ltrim
        """

        command: list = ["LTRIM", key, start, stop]

        return await self.run(command)

    async def rpop(self, key: str, count: int | None = None) -> (str | None) | list[str]:
        """
        See https://redis.io/commands/rpop

        :param count: defaults to 1 on the server side
        """

        command: list = ["RPOP", key]

        if count is not None:
            command.append(count)

        return await self.run(command)

    async def rpoplpush(self, source_key: str, destination_key: str) -> str | None:
        """
        See https://redis.io/commands/rpoplpush
        """

        command: list = ["RPOPLPUSH", source_key, destination_key]

        return await self.run(command)

    async def rpush(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/rpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: list = ["RPUSH", key, *elements]

        return await self.run(command)

    async def rpushx(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/rpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: list = ["RPUSHX", key, *elements]

        return await self.run(command)

    async def publish(self, channel: str, message: str) -> int:
        """
        See https://redis.io/commands/publish
        """

        command: list = ["PUBLISH", channel, message]

        return await self.run(command)

    async def pubsub(self) -> "PubSub":
        """
        See https://redis.io/commands/pubsub
        """

        return PubSub(client=self)

    async def eval(self, script: str, keys: list[str] | None = None, arguments: list | None = None) -> Any:
        """
        See https://redis.io/commands/eval

        The number of keys is calculated automatically.
        """

        command: list = ["EVAL", script]

        if keys:
            command.extend([len(keys), *keys])

        if arguments:
            command.extend(arguments)

        return await self.run(command)

    async def evalsha(
        self,
        sha1_digest: str,
        keys: list[str] | None = None,
        arguments: list | None = None
    ) -> Any:
        """
        See https://redis.io/commands/evalsha

        The number of keys is calculated automatically.
        """

        command: list = ["EVALSHA", sha1_digest]

        if keys:
            command.extend([len(keys), *keys])

        if arguments:
            command.extend(arguments)

        return await self.run(command)

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

        return await self.run(command)

    async def flushall(self, mode: Literal["ASYNC", "SYNC"] | None = None) -> str:
        """
        See https://redis.io/commands/flushall
        """

        command: list = ["FLUSHALL"]

        if mode:
            command.append(mode)

        return await self.run(command)

    async def flushdb(self, mode: Literal["ASYNC", "SYNC"] | None = None) -> str:
        """
        See https://redis.io/commands/flushdb
        """

        command: list = ["FLUSHDB"]

        if mode:
            command.append(mode)

        return await self.run(command)

    async def time(self) -> list[str] | dict[str, int]:
        """
        See https://redis.io/commands/time

        :return: A dict with the keys "seconds" and "microseconds" if self.format_return is True.
        """

        command: list = ["TIME"]

        raw: list[str] = await self.run(command)

        return format_server_time_return(raw) if self.format_return else raw

    async def sadd(self, key: str, *members: Any) -> int:
        """
        See https://redis.io/commands/sadd
        """

        if len(members) == 0:
            raise Exception("At least one member must be added.")

        command: list = ["SADD", key, *members]

        return await self.run(command)

    async def scard(self, key: str) -> int:
        """
        See https://redis.io/commands/scard
        """

        command: list = ["SCARD", key]

        return await self.run(command)

    async def sdiff(self, *keys: str) -> list[str]:
        """
        See https://redis.io/commands/sdiff
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["SDIFF", *keys]

        return await self.run(command)

    async def sdiffstore(self, destination_key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/sdiffstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["SDIFFSTORE", destination_key, *keys]

        return await self.run(command)

    async def sinter(self, *keys: str) -> list[str]:
        """
        See https://redis.io/commands/sinter
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["SINTER", *keys]

        return await self.run(command)

    async def sinterstore(self, destination_key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/sinterstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["SINTERSTORE", destination_key, *keys]

        return await self.run(command)

    async def sismember(self, key: str, member: Any) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/sismember

        :return: A bool if self.format_return is True.
        """

        command: list = ["SISMEMBER", key, member]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def smembers(self, key: str) -> list[str]:
        """
        See https://redis.io/commands/smembers
        """

        command: list = ["SMEMBERS", key]

        return await self.run(command)

    async def smove(self, source_key: str, destination_key: str, member: Any) -> Literal[1, 0] | bool:
        """
        See https://redis.io/commands/smove

        :return: A bool if self.format_return is True.
        """

        command: list = ["SMOVE", source_key, destination_key, member]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def spop(self, key: str, count: int | None = None) -> (str | None) | list[str]:
        """
        See https://redis.io/commands/spop

        :param count: defaults to 1 on the server side
        """

        command: list = ["SPOP", key]

        if count is not None:
            command.append(count)

        return await self.run(command)

    async def srandmember(self, key: str, count: int | None = None) -> (str | None) | list[str]:
        """
        See https://redis.io/commands/srandmember

        :param count: defaults to 1 on the server side
        """

        command: list = ["SRANDMEMBER", key]

        if count is not None:
            command.append(count)

        return await self.run(command)

    async def srem(self, key: str, *members: Any) -> int:
        """
        See https://redis.io/commands/srem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: list = ["SREM", key, *members]

        return await self.run(command)

    async def sscan(
        self,
        key: str,
        cursor: int,
        pattern: str | None = None,
        count: int | None = None,
        return_cursor: bool = True
    ) -> (list[str | list[str]] | list[int | list[str]]) | list[str]:
        """
        See https://redis.io/commands/sscan

        :param pattern: replacement for "MATCH"

        :param count: defaults to 10 on the server side

        :param return_cursor: if set to False, it won't return the cursor

        :return: The cursor will be an integer if "format_return" is True.
        Only the list of elements will be returned if "return_cursor" is False.
        """

        command: list = ["SSCAN", key, cursor]

        if pattern is not None:
            command.extend(["MATCH", pattern])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the list of elements.
        raw: list[str | list[str]] = await self.run(command)

        if return_cursor:
            return [int(raw[0]), raw[1]] if self.format_return else raw

        return raw[1]

    async def sunion(self, *keys: str) -> list[str]:
        """
        See https://redis.io/commands/sunion
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["SUNION", *keys]

        return await self.run(command)

    async def sunionstore(self, destination_key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/sunionstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["SUNIONSTORE", destination_key, *keys]

        return await self.run(command)

    async def zadd(
        self,
        key: str,
        sorted_set_members: dict,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
        incr: bool = False
    ) -> int | (str | None | float):
        """
        See https://redis.io/commands/zadd

        :param sorted_set_members: a dict containing their names and scores.

        :return: A float representing the number of elements added or None if "incr" is False
        and "format_return" is True.
        """

        if nx and xx:
            raise Exception("\"nx\" and \"xx\" are mutually exclusive.")

        if gt and lt:
            raise Exception("\"gt\" and \"lt\" are mutually exclusive.")

        if nx and (gt or lt):
            raise Exception("\"nx\" and \"gt\" or \"lt\" are mutually exclusive.")

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

            raw: (str | None) = await self.run(command)

            return float(raw) if self.format_return and raw is not None else raw

        for name, score in sorted_set_members.items():
            command.extend([score, name])

        return await self.run(command)

    async def zcard(self, key: str) -> int:
        """
        See https://redis.io/commands/zcard
        """

        command: list = ["ZCARD", key]

        return await self.run(command)

    async def zcount(self, key: str, min_score: FloatMinMax, max_score: FloatMinMax) -> int:
        """
        See https://redis.io/commands/zcount

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: list = ["ZCOUNT", key, min_score, max_score]

        return await self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be list[str].
    """
    async def zdiff(self, *keys: str, with_scores: bool = False) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zdiff

        The number of keys is calculated automatically.

        :return: A dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["ZDIFF", len(keys), *keys]

        if with_scores:
            command.append("WITHSCORES")

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zdiffstore(self, destination_key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/zdiffstore

        The number of keys is calculated automatically.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["ZDIFFSTORE", destination_key, len(keys), *keys]

        return await self.run(command)

    async def zincrby(self, key: str, increment: float, member: str) -> str | float:
        """
        See https://redis.io/commands/zincrby

        :return: A float if "format_return" is True.
        """

        command: list = ["ZINCRBY", key, increment, member]

        raw: str = await self.run(command)

        return float(raw) if self.format_return else raw

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be list[str].
    """
    async def zinter(
        self,
        *keys: str,
        multiplication_factors: list[float] | None = None,
        aggregate: Literal["SUM", "MIN", "MAX"] | None = None,
        with_scores: bool = False
    ) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zinter

        The number of keys is calculated automatically.

        :param multiplication_factors: replacement for "WEIGHTS"

        :return: A dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["ZINTER", len(keys), *keys]

        if multiplication_factors:
            command.extend(["WEIGHTS", *multiplication_factors])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        if with_scores:
            command.append("WITHSCORES")

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zinterstore(
        self,
        destination_key: str,
        *keys: str,
        multiplication_factors: list[float] | None = None,
        aggregate: Literal["SUM", "MIN", "MAX"] | None = None
    ) -> int:
        """
        See https://redis.io/commands/zinterstore

        The number of keys is calculated automatically.

        :param multiplication_factors: replacement for "WEIGHTS"
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["ZINTERSTORE", destination_key, len(keys), *keys]

        if multiplication_factors:
            command.extend(["WEIGHTS", *multiplication_factors])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        return await self.run(command)

    async def zlexcount(self, key: str, min_score: str, max_score: str) -> int:
        """
        See https://redis.io/commands/zlexcount
        """

        if not min_score.startswith(('(', '[', '+', '-')) or not max_score.startswith(('(', '[', '+', '-')):
            raise Exception(
                "\"min_score\" and \"max_score\" must either start with '(' or '[' or be '+' or '-'."
            )

        command: list = ["ZLEXCOUNT", key, min_score, max_score]

        return await self.run(command)

    async def zmscore(self, key: str, *members: str) -> list[str | None] | list[float | None]:
        """
        See https://redis.io/commands/zmscore

        :return: A list of float or None values if "format_return" is True.
        """

        if len(members) == 0:
            raise Exception("At least one member must be specified.")

        command: list = ["ZMSCORE", key, *members]

        raw: list[str | None] = await self.run(command)

        return format_float_list(raw) if self.format_return else raw

    async def zpopmax(self, key: str, count: int | None = None) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zpopmax

        :param count: defaults to 1 on the server side

        :return: A dict of member-score pairs if "format_return" is True.
        """

        command: list = ["ZPOPMAX", key]

        if count is not None:
            command.append(count)

        raw: SortedSetReturn = await self.run(command)

        return format_sorted_set_return(raw) if self.format_return else raw

    async def zpopmin(self, key: str, count: int | None = None) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zpopmin

        :param count: defaults to 1 on the server side

        :return: A dict of member-score pairs if "format_return" is True.
        """

        command: list = ["ZPOPMIN", key]

        if count is not None:
            command.append(count)

        raw: SortedSetReturn = await self.run(command)

        return format_sorted_set_return(raw) if self.format_return else raw

    async def zrandmember(
        self,
        key: str,
        count: int | None = None,
        with_scores: bool = False
    ) -> (str | None) | (SortedSetReturn | FormattedSortedSetReturn):
        """
        See https://redis.io/commands/zrandmember

        :param count: defaults to 1 on the server side

        :return: A dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        if count is None and with_scores:
            raise Exception("\"with_scores\" can only be used with \"count\".")

        command: list = ["ZRANDMEMBER", key]

        if count is not None:
            command.append(count)

            if with_scores:
                command.append("WITHSCORES")

                raw: SortedSetReturn = await self.run(command)

                return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be list[str].
    """
    async def zrange(
        self,
        key: str,
        start: FloatMinMax,
        stop: FloatMinMax,
        range_method: Literal["BYSCORE", "BYLEX"] | None = None,
        rev: bool = False,
        offset: int | None = None,
        count: int | None = None,
        with_scores: bool = False
    ) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zrange

        If you need to use "-inf" and "+inf", please write them as strings.

        :return: A dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        handle_non_deprecated_zrange_exceptions(range_method, start, stop, offset, count)

        command: list = ["ZRANGE", key, start, stop]

        if range_method:
            command.append(range_method)

        if rev:
            command.append("REV")

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if with_scores:
            command.append("WITHSCORES")

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zrangebylex(
        self,
        key: str,
        min_score: str,
        max_score: str,
        offset: int | None = None,
        count: int | None = None
    ) -> list[str | None]:
        """
        See https://redis.io/commands/zrangebylex
        """

        handle_zrangebylex_exceptions(min_score, max_score, offset, count)

        command: list = ["ZRANGEBYLEX", key, min_score, max_score]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return await self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be list[str].
    """
    async def zrangebyscore(
        self,
        key: str,
        min_score: FloatMinMax,
        max_score: FloatMinMax,
        with_scores: bool = False,
        offset: int | None = None,
        count: int | None = None
    ) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zrangebyscore

        If you need to use "-inf" and "+inf", please write them as strings.

        :return: A dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        if number_are_not_none(offset, count, number=1):
            raise Exception("Both \"offset\" and \"count\" must be specified.")

        command: list = ["ZRANGEBYSCORE", key, min_score, max_score]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if with_scores:
            command.append("WITHSCORES")

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zrangestore(
        self,
        destination_key: str,
        source_key: str,
        start: FloatMinMax,
        stop: FloatMinMax,
        range_method: Literal["BYSCORE", "BYLEX"] | None = None,
        rev: bool = False,
        offset: int | None = None,
        count: int | None = None
    ) -> int:
        """
        See https://redis.io/commands/zrangestore

        If you need to use "-inf" and "+inf", please write them as strings.

        :param start: replacement for "min" to match "ZRANGE"
        :param stop: replacement for "max" to match "ZRANGE"
        """

        handle_non_deprecated_zrange_exceptions(range_method, start, stop, offset, count)

        command: list = ["ZRANGESTORE", destination_key, source_key, start, stop]

        if range_method:
            command.append(range_method)

        if rev:
            command.append("REV")

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return await self.run(command)

    async def zrank(self, key: str, member: str) -> int | None:
        """
        See https://redis.io/commands/zrank
        """

        command: list = ["ZRANK", key, member]

        return await self.run(command)

    async def zrem(self, key: str, *members: str) -> int:
        """
        See https://redis.io/commands/zrem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: list = ["ZREM", key, *members]

        return await self.run(command)

    async def zremrangebylex(self, key: str, min_score: str, max_score: str) -> int:
        """
        See https://redis.io/commands/zremrangebylex
        """

        if not min_score.startswith(('(', '[', '+', '-')) or not max_score.startswith(('(', '[', '+', '-')):
            raise Exception(
                "\"min_score\" and \"max_score\" must either start with '(' or '[' or be '+' or '-'."
            )

        command: list = ["ZREMRANGEBYLEX", key, min_score, max_score]

        return await self.run(command)

    async def zremrangebyrank(self, key: str, start: int, stop: int) -> int:
        """
        See https://redis.io/commands/zremrangebyrank
        """

        command: list = ["ZREMRANGEBYRANK", key, start, stop]

        return await self.run(command)

    async def zremrangebyscore(self, key: str, min_score: FloatMinMax, max_score: FloatMinMax) -> int:
        """
        See https://redis.io/commands/zremrangebyscore

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: list = ["ZREMRANGEBYSCORE", key, min_score, max_score]

        return await self.run(command)

    """
    This has actually 3 return scenarios, but,
    whether "with_scores" is True or not, its raw return type will be list[str].
    """
    async def zrevrange(
        self,
        key: str,
        start: int,
        stop: int,
        with_scores: bool = False
    ) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zrevrange

        :return: A dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        command: list = ["ZREVRANGE", key, start, stop]

        if with_scores:
            command.append("WITHSCORES")

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zrevrangebylex(
        self,
        key: str,
        max_score: str,
        min_score: str,
        offset: int | None = None,
        count: int | None = None
    ) -> list[str]:
        """
        See https://redis.io/commands/zrevrangebylex
        """

        handle_zrangebylex_exceptions(min_score, max_score, offset, count)

        command: list = ["ZREVRANGEBYLEX", key, max_score, min_score]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return await self.run(command)

    """
    This has actually 3 return scenarios, but,
    whether "with_scores" is True or not, its raw return type will be list[str].
    """
    async def zrevrangebyscore(
        self,
        key: str,
        max_score: FloatMinMax,
        min_score: FloatMinMax,
        with_scores: bool = False,
        offset: int | None = None,
        count: int | None = None
    ) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zrevrangebyscore

        If you need to use "-inf" and "+inf", please write them as strings.

        :return: A dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        if number_are_not_none(offset, count, number=1):
            raise Exception("Both \"offset\" and \"count\" must be specified.")

        command: list = ["ZREVRANGEBYSCORE", key, max_score, min_score]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if with_scores:
            command.append("WITHSCORES")

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zrevrank(self, key: str, member: str) -> int | None:
        """
        See https://redis.io/commands/zrevrank
        """

        command: list = ["ZREVRANK", key, member]

        return await self.run(command)

    async def zscan(
        self,
        key: str,
        cursor: int,
        pattern: str | None = None,
        count: int | None = None,
        return_cursor: bool = True
    ) -> (
            list[str | SortedSetReturn] | list[int | FormattedSortedSetReturn]
         ) | (
            SortedSetReturn | FormattedSortedSetReturn
         ):
        """
        See https://redis.io/commands/zscan

        :param pattern: replacement for "MATCH"

        :param count: defaults to 10 on the server side

        :param return_cursor: if set to False, it won't return the cursor

        :return: The cursor will be an integer if "format_return" is True.
        Only a dict of member-score pairs will be returned if "return_cursor" is False and "format_return" is True.
        """

        command: list = ["ZSCAN", key, cursor]

        if pattern is not None:
            command.extend(["MATCH", pattern])

        if count is not None:
            command.extend(["COUNT", count])

        raw: list[int | SortedSetReturn] = await self.run(command)

        if return_cursor:
            return [int(raw[0]), format_sorted_set_return(raw[1])] if self.format_return else raw

        # The raw result is composed of the new cursor and the list of elements.
        return format_sorted_set_return(raw[1]) if self.format_return else raw[1]

    async def zscore(self, key: str, member: str) -> str | None | float:
        """
        See https://redis.io/commands/zscore

        :return: A float or None if "format_return" is True.
        """

        command: list = ["ZSCORE", key, member]

        raw: str | None = await self.run(command)

        return float(raw) if self.format_return and raw is not None else raw

    """
    This has actually 3 return scenarios, but,
    whether "with_scores" is True or not, its raw return type will be list[str].
    """
    async def zunion(
        self,
        *keys: str,
        multiplication_factors: list[float] | None = None,
        aggregate: Literal["SUM", "MIN", "MAX"] | None = None,
        with_scores: bool = False
    ) -> SortedSetReturn | FormattedSortedSetReturn:
        """
        See https://redis.io/commands/zunion

        The number of keys is calculated automatically.

        :param multiplication_factors: replacement for "WEIGHTS"

        :return: A dict of member-score pairs if "with_scores" and "format_return" are True.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["ZUNION", len(keys), *keys]

        if multiplication_factors:
            command.extend(["WEIGHTS", *multiplication_factors])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        if with_scores:
            command.append("WITHSCORES")

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zunionstore(
        self,
        destination_key: str,
        *keys: str,
        multiplication_factors: list[float] | None = None,
        aggregate: Literal["SUM", "MIN", "MAX"] | None = None
    ) -> int:
        """
        See https://redis.io/commands/zunionstore

        The number of keys is calculated automatically.

        :param multiplication_factors: replacement for "WEIGHTS"
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["ZUNIONSTORE", destination_key, len(keys), *keys]

        if multiplication_factors:
            command.extend(["WEIGHTS", *multiplication_factors])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        return await self.run(command)

    async def append(self, key: str, value: Any) -> int:
        """
        See https://redis.io/commands/append
        """

        command: list = ["APPEND", key, value]

        return await self.run(command)

    async def decr(self, key: str) -> int:
        """
        See https://redis.io/commands/decr
        """

        command: list = ["DECR", key]

        return await self.run(command)

    async def decrby(self, key: str, decrement: int) -> int:
        """
        See https://redis.io/commands/decrby
        """

        command: list = ["DECRBY", key, decrement]

        return await self.run(command)

    async def get(self, key: str) -> str | None:
        """
        See https://redis.io/commands/get
        """

        command: list = ["GET", key]

        return await self.run(command)

    async def getdel(self, key: str) -> str | None:
        """
        See https://redis.io/commands/getdel
        """

        command: list = ["GETDEL", key]

        return await self.run(command)

    async def getex(
        self,
        key: str,
        seconds: int | None = None,
        milliseconds: int | None = None,
        unix_time_seconds: int | None = None,
        unix_time_milliseconds: int | None = None,
        persist: bool | None = None
    ) -> str | None:
        """
        See https://redis.io/commands/getex
        """

        if not number_are_not_none(seconds, milliseconds, unix_time_seconds, unix_time_milliseconds, persist, number=1):
            raise Exception("Exactly one of the expiration settings must be specified.")

        command: list = ["GETEX", key]

        if seconds is not None:
            command.extend(["EX", seconds])

        if milliseconds is not None:
            command.extend(["PX", milliseconds])

        if unix_time_seconds is not None:
            command.extend(["EXAT", unix_time_seconds])

        if unix_time_milliseconds is not None:
            command.extend(["PXAT", unix_time_milliseconds])

        if persist is not None:
            command.append("PERSIST")

        return await self.run(command)

    async def getrange(self, key: str, start: int, end: int) -> str:
        """
        See https://redis.io/commands/getrange
        """

        command: list = ["GETRANGE", key, start, end]

        return await self.run(command)

    async def getset(self, key: str, value: Any) -> str | None:
        """
        See https://redis.io/commands/getset
        """

        command: list = ["GETSET", key, value]

        return await self.run(command)

    async def incr(self, key: str) -> int:
        """
        See https://redis.io/commands/incr
        """

        command: list = ["INCR", key]

        return await self.run(command)

    async def incrby(self, key: str, increment: int) -> int:
        """
        See https://redis.io/commands/incrby
        """

        command: list = ["INCRBY", key, increment]

        return await self.run(command)

    async def incrbyfloat(self, key: str, increment: float) -> str | float:
        """
        See https://redis.io/commands/incrbyfloat

        :return: A float if "format_return" is True.
        """

        command: list = ["INCRBYFLOAT", key, increment]

        raw: str = await self.run(command)

        return float(raw) if self.format_return else raw

    async def mget(self, *keys: str) -> list[str | None]:
        """
        See https://redis.io/commands/mget
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: list = ["MGET", *keys]

        return await self.run(command)

    async def mset(self, keys_and_values: dict) -> Literal["OK"]:
        """
        See https://redis.io/commands/mset
        """

        command: list = ["MSET"]

        for key, value in keys_and_values.items():
            command.extend([key, value])

        return await self.run(command)

    async def msetnx(self, keys_and_values: dict) -> Literal[1, 0]:
        """
        See https://redis.io/commands/msetnx
        """

        command: list = ["MSETNX"]

        for key, value in keys_and_values.items():
            command.extend([key, value])

        return await self.run(command)

    async def psetex(self, key: str, milliseconds: int, value: Any) -> str:
        """
        See https://redis.io/commands/psetex
        """

        command: list = ["PSETEX", key, milliseconds, value]

        return await self.run(command)

    async def set(
        self,
        key: str,
        value: Any,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        seconds: int | None = None,
        milliseconds: int | None = None,
        unix_time_seconds: int | None = None,
        unix_time_milliseconds: int | None = None,
        keep_ttl: bool = False,
    ) -> str | None:
        """
        See https://redis.io/commands/set
        """

        if nx and xx:
            raise Exception("\"nx\" and \"xx\" are mutually exclusive.")

        if not number_are_not_none(
            seconds,
            milliseconds,
            unix_time_seconds,
            unix_time_milliseconds,
            keep_ttl,
            number=1
        ):
            raise Exception("Exactly one of the expiration settings must be specified.")

        if nx and get:
            raise Exception("\"nx\" and \"get\" are mutually exclusive.")

        command: list = ["SET", key, value]

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        if get:
            command.append("GET")

        if seconds is not None:
            command.extend(["EX", seconds])

        if milliseconds is not None:
            command.extend(["PX", milliseconds])

        if unix_time_seconds is not None:
            command.extend(["EXAT", unix_time_seconds])

        if unix_time_milliseconds is not None:
            command.extend(["PXAT", unix_time_milliseconds])

        if keep_ttl:
            command.append("KEEPTTL")

        return await self.run(command)

    async def setex(self, key: str, seconds: int, value: Any) -> str:
        """
        See https://redis.io/commands/setex
        """

        command: list = ["SETEX", key, seconds, value]

        return await self.run(command)

    async def setnx(self, key: str, value: Any) -> Literal[1, 0]:
        """
        See https://redis.io/commands/setnx
        """

        command: list = ["SETNX", key, value]

        return await self.run(command)

    async def setrange(self, key: str, offset: int, value: Any) -> int:
        """
        See https://redis.io/commands/setrange
        """

        command: list = ["SETRANGE", key, offset, value]

        return await self.run(command)

    async def strlen(self, key: str) -> int:
        """
        See https://redis.io/commands/strlen
        """

        command: list = ["STRLEN", key]

        return await self.run(command)

    async def substr(self, key: str, start: int, end: int) -> str:
        """
        See https://redis.io/commands/substr
        """

        command: list = ["SUBSTR", key, start, end]

        return await self.run(command)


# It doesn't inherit from "Redis" mainly because of the methods signatures.
class BitFieldCommands:
    def __init__(self, client: Redis, key: str):
        self.client = client
        self.command: list = ["BITFIELD", key]

    def get(self, encoding: str, offset: BitFieldOffset):
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    def set(self, encoding: str, offset: BitFieldOffset, value: int):
        """
        Set the specified bit field and returns its old value.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["SET", encoding, offset, value]
        self.command.extend(_command)

        return self

    def incrby(self, encoding: str, offset: BitFieldOffset, increment: int):
        """
        Increments or decrements (if a negative increment is given) the specified bit field and returns the new value.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["INCRBY", encoding, offset, increment]
        self.command.extend(_command)

        return self

    def overflow(self, overflow: Literal["WRAP", "SAT", "FAIL"]):
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

    async def execute(self) -> list:
        return await self.client.run(command=self.command)


class BitFieldRO:
    def __init__(self, client: Redis, key: str):
        self.client = client
        self.command: list = ["BITFIELD_RO", key]

    def get(self, encoding: str, offset: BitFieldOffset):
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    async def execute(self) -> list:
        return await self.client.run(command=self.command)


class PubSub:
    def __init__(self, client: Redis):
        self.client = client
        self.command: list = ["PUBSUB"]

    async def channels(self, pattern: str | None = None) -> list[str]:
        """
        See https://redis.io/commands/pubsub-channels
        """

        self.command.append("CHANNELS")

        if pattern is not None:
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

        :return: A dict with channel-(number-of-subscribers) pairs if "format_return" is True.
        """

        self.command.extend(["NUMSUB", *channels])

        raw: list[str | int] = await self.client.run(command=self.command)

        return format_pubsub_numsub_return(raw) if self.client.format_return else raw


class Script:
    def __init__(self, client: Redis):
        self.client = client
        self.command: list = ["SCRIPT"]

    async def exists(self, *sha1_digests: str) -> list[Literal[1, 0]] | list[bool]:
        """
        See https://redis.io/commands/script-exists

        :return: A list of bools if "format_return" is True.
        """

        if len(sha1_digests) == 0:
            raise Exception("At least one sha1 digest must be provided.")

        self.command.extend(["EXISTS", *sha1_digests])

        raw: list[Literal[1, 0]] = await self.client.run(command=self.command)

        return format_bool_list(raw) if self.client.format_return else raw

    async def flush(self, mode: Literal["ASYNC", "SYNC"]) -> str:
        """
        See https://redis.io/commands/script-flush
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

        if category_name is not None:
            self.command.append(category_name)

        return await self.client.run(command=self.command)

    async def deluser(self, *usernames: str) -> int:
        # See https://redis.io/commands/acl-deluser
        
        if len(usernames) == 0:
            raise Exception("At least one username must be provided.")

        self.command.extend(["DELUSER", *usernames])

        return await self.client.run(command=self.command)

    async def genpass(self, bits: int | None = None) -> str:
        # See https://redis.io/commands/acl-genpass

        self.command.append("GENPASS")

        if bits is not None:
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

    async def log(self, count: int | None = None, reset: bool = False) -> list[str]:
        # See https://redis.io/commands/acl-log

        if count is not None and reset:
            raise Exception("Cannot specify both "count" and "reset".")

        self.command.append("LOG")

        if count is not None:
            self.command.append(count)

        if reset:
            self.command.append("RESET")

        return await self.client.run(command=self.command)
"""