from upstash_redis.http.execute import execute
from upstash_redis.schema.http import RESTResult, RESTEncoding
from upstash_redis.schema.telemetry import TelemetryData
from upstash_redis.config import (
    REST_ENCODING,
    REST_RETRIES,
    REST_RETRY_INTERVAL,
    ALLOW_TELEMETRY,
    FORMAT_RETURN,
)
from upstash_redis.utils.format import (
    format_geo_positions_return,
    format_geo_members_return,
    format_hash_return,
    format_pubsub_numsub_return,
    format_bool_list,
    format_server_time_return,
    format_sorted_set_return,
    format_float_list,
)
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
from upstash_redis.schema.commands.returns import (
    GeoMembersReturn,
    FormattedGeoMembersReturn,
    HashReturn,
    FormattedHashReturn,
    SortedSetReturn,
    FormattedSortedSetReturn,
)
from aiohttp import ClientSession
from typing import Type, Any, Literal, Union, List, Dict
from os import environ


class Commands:
    def __init__(
        self,
        url: str,
        token: str,
        rest_encoding: RESTEncoding = REST_ENCODING,
        rest_retries: int = REST_RETRIES,
        rest_retry_interval: int = REST_RETRY_INTERVAL,  # Seconds.
        format_return: bool = FORMAT_RETURN,
        allow_telemetry: bool = ALLOW_TELEMETRY,
        telemetry_data: Union[TelemetryData, None] = None,
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

        # See https://redis.io/commands/pubsub/
        self.pubsub = PubSub(client=self)

        # See https://redis.io/commands/script/
        self.script = Script(client=self)

        """
        Need to double-check compatibility with the classic Redis API for this one.
        
        See https://redis.io/commands/acl
        self.acl = ACL(client=self)
        """

    @classmethod
    def from_env(
        cls,
        rest_encoding: RESTEncoding = REST_ENCODING,
        rest_retries: int = REST_RETRIES,
        rest_retry_interval: int = REST_RETRY_INTERVAL,
        format_return: bool = FORMAT_RETURN,
        allow_telemetry: bool = ALLOW_TELEMETRY,
        telemetry_data: Union[TelemetryData, None] = None,
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
            telemetry_data,
        )

    async def __aenter__(self) -> ClientSession:
        """
        Enter the async context.
        """

        self._session: ClientSession = ClientSession()
        # It needs to return the session object because it will be used in "async with" statements.
        return self._session

    async def __aexit__(
        self,
        exc_type: Union[Type[BaseException], None],
        exc_val: Union[BaseException, None],
        exc_tb: Any,
    ) -> None:
        """
        Exit the async context.
        """

        await self._session.close()

    async def run(self, command: List) -> RESTResult:
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
            telemetry_data=self.telemetry_data,
        )

    async def bitcount(
        self, key: str, start: Union[int, None] = None, end: Union[int, None] = None
    ) -> int:
        """
        See https://redis.io/commands/bitcount
        """

        if number_are_not_none(start, end, number=1):
            raise Exception('Both "start" and "end" must be specified.')

        command: List = ["BITCOUNT", key]

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
        self, operation: Literal["AND", "OR", "XOR", "NOT"], destkey: str, *srckeys: str
    ) -> int:
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

        return await self.run(command)

    async def bitpos(
        self,
        key: str,
        bit: Literal[0, 1],
        start: Union[int, None] = None,
        end: Union[int, None] = None,
    ) -> int:
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

        return await self.run(command)

    async def getbit(self, key: str, offset: int) -> int:
        """
        See https://redis.io/commands/getbit
        """

        command: List = ["GETBIT", key, offset]

        return await self.run(command)

    async def setbit(self, key: str, offset: int, value: Literal[0, 1]) -> int:
        """
        See https://redis.io/commands/setbit
        """

        command: List = ["SETBIT", key, offset, value]

        return await self.run(command)

    async def ping(self, message: Union[str, None] = None) -> str:
        """
        See https://redis.io/commands/ping
        """

        command: List = ["PING"]

        if message is not None:
            command.append(message)

        return await self.run(command)

    async def echo(self, message: str) -> str:
        """
        See https://redis.io/commands/echo
        """

        command: List = ["ECHO", message]

        return await self.run(command)

    async def copy(
        self, source: str, destination: str, replace: bool = False
    ) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/copy

        :return: A bool if "format_return" is True.
        """

        command: List = ["COPY", source, destination]

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

        command: List = ["DEL", *keys]

        return await self.run(command)

    async def exists(self, *keys: str) -> int:
        """
        See https://redis.io/commands/exists
        """

        if len(keys) == 0:
            raise Exception("At least one key must be checked.")

        command: List = ["EXISTS", *keys]

        return await self.run(command)

    async def expire(self, key: str, seconds: int) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/expire

        :return: A bool if "format_return" is True.
        """

        command: List = ["EXPIRE", key, seconds]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def expireat(
        self, key: str, unix_time_seconds: int
    ) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/expireat

        :return: A bool if "format_return" is True.
        """

        command: List = ["EXPIREAT", key, unix_time_seconds]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def keys(self, pattern: str) -> List[str]:
        """
        See https://redis.io/commands/keys
        """

        command: List = ["KEYS", pattern]

        return await self.run(command)

    async def persist(self, key: str) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/persist

        :return: A bool if "format_return" is True.
        """

        command: List = ["PERSIST", key]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def pexpire(self, key: str, milliseconds: int) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/pexpire

        :return: A bool if "format_return" is True.
        """

        command: List = ["PEXPIRE", key, milliseconds]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def pexpireat(
        self, key: str, unix_time_milliseconds: int
    ) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/pexpireat

        :return: A bool if "format_return" is True.
        """

        command: List = ["PEXPIREAT", key, unix_time_milliseconds]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def pttl(self, key: str) -> int:
        """
        See https://redis.io/commands/pttl
        """

        command: List = ["PTTL", key]

        return await self.run(command)

    async def randomkey(self) -> Union[str, None]:
        """
        See https://redis.io/commands/randomkey
        """

        command: List = ["RANDOMKEY"]

        return await self.run(command)

    async def rename(self, key: str, newkey: str) -> str:
        """
        See https://redis.io/commands/rename
        """

        command: List = ["RENAME", key, newkey]

        return await self.run(command)

    async def renamenx(self, key: str, newkey: str) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/renamenx

        :return: A bool if "format_return" is True.
        """

        command: List = ["RENAMENX", key, newkey]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def scan(
        self,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
        scan_type: Union[str, None] = None,
    ) -> Union[List[Union[str, List[str]]], List[Union[int, List[str]]]]:
        """
        See https://redis.io/commands/scan

        :param return_cursor: if set to False, it won't return the cursor

        :param scan_type: replacement for "TYPE"
        :param match_pattern: replacement for "MATCH"

        :return: The cursor will be an integer if "format_return" is True.
        Only the List of elements will be returned if "return_cursor" is False
        """

        command: List = ["SCAN", cursor]

        if match_pattern is not None:
            command.extend(["MATCH", match_pattern])

        if count is not None:
            command.extend(["COUNT", count])

        if scan_type is not None:
            command.extend(["TYPE", scan_type])

        # The raw result is composed of the new cursor and the List of elements.
        raw: List[Union[str, List[str]]] = await self.run(command)

        return [int(raw[0]), raw[1]] if self.format_return else raw

    async def touch(self, *keys: str) -> int:
        """
        See https://redis.io/commands/touch
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["TOUCH", *keys]

        return await self.run(command)

    async def ttl(self, key: str) -> int:
        """
        See https://redis.io/commands/ttl
        """

        command: List = ["TTL", key]

        return await self.run(command)

    async def type(self, key: str) -> Union[str, None]:
        """
        See https://redis.io/commands/type
        """

        command: List = ["TYPE", key]

        return await self.run(command)

    async def unlink(self, *keys: str) -> int:
        """
        See https://redis.io/commands/unlink
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["UNLINK", *keys]

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

        return await self.run(command)

    async def geodist(
        self,
        key: str,
        member1: str,
        member2: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"] = "M",
    ) -> Union[str, float, None]:
        """
        See https://redis.io/commands/geodist

        :return: A float value if "format_return" is True.
        """

        command: List = ["GEODIST", key, member1, member2, unit]

        raw: Union[str, None] = await self.run(command)

        return float(raw) if self.format_return else raw

    async def geohash(self, key: str, *members: str) -> List[Union[str, None]]:
        """
        See https://redis.io/commands/geohash
        """

        command: List = ["GEOHASH", key, *members]

        return await self.run(command)

    async def geopos(
        self, key: str, *members: str
    ) -> Union[List[Union[List[str], None]], List[Union[Dict[str, float], None]]]:
        """
        See https://redis.io/commands/geopos

        :return: A List of Dicts with either None or the longitude and latitude if "format_return" is True.
        """

        command: List = ["GEOPOS", key, *members]

        raw: List[Union[List[str], None]] = await self.run(command)

        return format_geo_positions_return(raw) if self.format_return else raw

    async def georadius(
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
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn, int]:
        """
        See https://redis.io/commands/georadius

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True.
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

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "List[str]".
        if self.format_return and (withdist or withhash or withcoord):
            return format_geo_members_return(raw, withdist, withhash, withcoord)

        return raw

    async def georadius_ro(
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
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn]:
        """
        See https://redis.io/commands/georadius_ro

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True.
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

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "List[str]".
        if self.format_return and (withdist or withhash or withcoord):
            return format_geo_members_return(raw, withdist, withhash, withcoord)

        return raw

    async def georadiusbymember(
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
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn]:
        """
        See https://redis.io/commands/georadiusbymember

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True.
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

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "List[str]".
        if self.format_return and (withdist or withhash or withcoord):
            return format_geo_members_return(raw, withdist, withhash, withcoord)

        return raw

    async def georadiusbymember_ro(
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
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn]:
        """
        See https://redis.io/commands/georadiusbymember_ro

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True.
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

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "List[str]".
        if self.format_return and (withdist or withhash or withcoord):
            return format_geo_members_return(raw, withdist, withhash, withcoord)

        return raw

    async def geosearch(
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
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn]:
        """
        See https://redis.io/commands/geosearch

        :param count_any: replacement for "ANY"

        :return: A List of Dicts with the requested properties if "format_return" is True.
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

        raw: GeoMembersReturn = await self.run(command)

        # If none of the additional properties are requested, the result will be "List[str]".
        if self.format_return and (withdist or withhash or withcoord):
            return format_geo_members_return(raw, withdist, withhash, withcoord)

        return raw

    async def geosearchstore(
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
    ) -> int:
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

        return await self.run(command)

    async def hdel(self, key: str, *fields: str) -> int:
        """
        See https://redis.io/commands/hdel
        """

        if len(fields) == 0:
            raise Exception("At least one field must be deleted.")

        command: List = ["HDEL", key, *fields]

        return await self.run(command)

    async def hexists(self, key: str, field: str) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/hexists

        :return: A bool if "format_return" is True.
        """

        command: List = ["HEXISTS", key, field]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def hget(self, key: str, field: str) -> Union[str, None]:
        """
        See https://redis.io/commands/hget
        """

        command: List = ["HGET", key, field]

        return await self.run(command)

    async def hgetall(self, key: str) -> Union[HashReturn, FormattedHashReturn]:
        """
        See https://redis.io/commands/hgetall

        :return: A Dict of field-value pairs if "format_return" is True.
        """

        command: List = ["HGETALL", key]

        raw: HashReturn = await self.run(command)

        return format_hash_return(raw) if self.format_return else raw

    async def hincrby(self, key: str, field: str, increment: int) -> int:
        """
        See https://redis.io/commands/hincrby
        """

        command: List = ["HINCRBY", key, field, increment]

        return await self.run(command)

    async def hincrbyfloat(
        self, key: str, field: str, increment: float
    ) -> Union[str, float]:
        """
        See https://redis.io/commands/hincrbyfloat

        :return: A float if "format_return" is True.
        """

        command: List = ["HINCRBYFLOAT", key, field, increment]

        raw: str = await self.run(command)

        return float(raw) if self.format_return else raw

    async def hkeys(self, key: str) -> List[str]:
        """
        See https://redis.io/commands/hkeys
        """

        command: List = ["HKEYS", key]

        return await self.run(command)

    async def hlen(self, key: str) -> int:
        """
        See https://redis.io/commands/hlen
        """

        command: List = ["HLEN", key]

        return await self.run(command)

    async def hmget(self, key: str, *fields: str) -> List[Union[str, None]]:
        """
        See https://redis.io/commands/hmget
        """

        if len(fields) == 0:
            raise Exception("At least one field must be specified.")

        command: List = ["HMGET", key, *fields]

        return await self.run(command)

    async def hmset(self, key: str, field_value_pairs: Dict) -> str:
        """
        See https://redis.io/commands/hmset
        """

        command: List = ["HMSET", key]

        for field, value in field_value_pairs.items():
            command.extend([field, value])

        return await self.run(command)

    async def hrandfield(
        self, key: str, count: Union[int, None] = None, withvalues: bool = False
    ) -> Union[(Union[str, None]), Union[HashReturn, FormattedHashReturn]]:
        """
        See https://redis.io/commands/hrandfield

        :return: A Dict of field-value pairs if "count" and "withvalues" are specified and "format_return" is True.
        """

        if count is None and withvalues:
            raise Exception('"withvalues" can only be used together with "count"')

        command: List = ["HRANDFIELD", key]

        if count is not None:
            command.extend(["COUNT", count])

            if withvalues:
                command.append("WITHVALUES")

                raw: HashReturn = await self.run(command)

                return format_hash_return(raw) if self.format_return else raw

        return await self.run(command)

    async def hscan(
        self,
        key: str,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> Union[List[Union[str, HashReturn]], List[Union[int, FormattedHashReturn]]]:
        """
        See https://redis.io/commands/hscan

        :param return_cursor: if set to False, it won't return the cursor
        :param match_pattern: replacement for "MATCH"

        :return: The cursor will be an integer if "format_return" is True.
        Only a Dict of field-value pairs will be returned if "return_cursor" is False and "format_return" is True.
        """

        command: List = ["HSCAN", key, cursor]

        if match_pattern is not None:
            command.extend(["MATCH", match_pattern])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        raw: Union[List[Union[str, HashReturn]], HashReturn] = await self.run(command)

        return [int(raw[0]), format_hash_return(raw[1])] if self.format_return else raw


    async def hset(self, key: str, field_value_pairs: Dict) -> int:
        """
        See https://redis.io/commands/hset
        """

        command: List = ["HSET", key]

        for field, value in field_value_pairs.items():
            command.extend([field, value])

        return await self.run(command)

    async def hsetnx(
        self, key: str, field: str, value: Any
    ) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/hsetnx

        :return: A bool if "format_return" is True.
        """

        command: List = ["HSETNX", key, field, value]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def hstrlen(self, key: str, field: str) -> int:
        """
        See https://redis.io/commands/hstrlen
        """

        command: List = ["HSTRLEN", key, field]

        return await self.run(command)

    async def hvals(self, key: str) -> List[str]:
        """
        See https://redis.io/commands/hvals
        """

        command: List = ["HVALS", key]

        return await self.run(command)

    async def pfadd(self, key: str, *elements: Any) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/pfadd

        :return: A bool if "format_return" is True.
        """

        command: List = ["PFADD", key, *elements]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def pfcount(self, *keys: str) -> int:
        """
        See https://redis.io/commands/pfcount
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["PFCOUNT", *keys]

        return await self.run(command)

    async def pfmerge(self, destkey: str, *sourcekeys: str) -> str:
        """
        See https://redis.io/commands/pfmerge
        """

        command: List = ["PFMERGE", destkey, *sourcekeys]

        return await self.run(command)

    async def lindex(self, key: str, index: int) -> Union[str, None]:
        """
        See https://redis.io/commands/lindex
        """

        command: List = ["LINDEX", key, index]

        return await self.run(command)

    async def linsert(
        self, key: str, position: Literal["BEFORE", "AFTER"], pivot: Any, element: Any
    ) -> int:
        """
        See https://redis.io/commands/linsert
        """

        command: List = ["LINSERT", key, position, pivot, element]

        return await self.run(command)

    async def llen(self, key: str) -> int:
        """
        See https://redis.io/commands/llen
        """

        command: List = ["LLEN", key]

        return await self.run(command)

    async def lmove(
        self,
        source: str,
        destination: str,
        source_position: Literal["LEFT", "RIGHT"],
        destination_position: Literal["LEFT", "RIGHT"],
    ) -> Union[str, None]:
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

        return await self.run(command)

    async def lpop(
        self, key: str, count: Union[int, None] = None
    ) -> Union[(Union[str, None]), List[str]]:
        """
        See https://redis.io/commands/lpop

        :param count: defaults to 1 on the server side
        """

        command: List = ["LPOP", key]

        if count is not None:
            command.append(count)

        return await self.run(command)

    async def lpos(
        self,
        key: str,
        element: Any,
        rank: Union[int, None] = None,
        count: Union[int, None] = None,
        maxlen: Union[int, None] = None,
    ) -> Union[(Union[int, None]), List[int]]:
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

        return await self.run(command)

    async def lpush(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/lpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["LPUSH", key, *elements]

        return await self.run(command)

    async def lpushx(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/lpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["LPUSHX", key, *elements]

        return await self.run(command)

    async def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """
        See https://redis.io/commands/lrange
        """

        command: List = ["LRANGE", key, start, stop]

        return await self.run(command)

    async def lrem(self, key: str, count: int, element: Any) -> int:
        """
        See https://redis.io/commands/lrem
        """

        command: List = ["LREM", key, count, element]

        return await self.run(command)

    async def lset(self, key: str, index: int, element: Any) -> str:
        """
        See https://redis.io/commands/lset
        """

        command: List = ["LSET", key, index, element]

        return await self.run(command)

    async def ltrim(self, key: str, start: int, stop: int) -> str:
        """
        See https://redis.io/commands/ltrim
        """

        command: List = ["LTRIM", key, start, stop]

        return await self.run(command)

    async def rpop(
        self, key: str, count: Union[int, None] = None
    ) -> Union[(Union[str, None]), List[str]]:
        """
        See https://redis.io/commands/rpop

        :param count: defaults to 1 on the server side
        """

        command: List = ["RPOP", key]

        if count is not None:
            command.append(count)

        return await self.run(command)

    async def rpoplpush(self, source: str, destination: str) -> Union[str, None]:
        """
        See https://redis.io/commands/rpoplpush
        """

        command: List = ["RPOPLPUSH", source, destination]

        return await self.run(command)

    async def rpush(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/rpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["RPUSH", key, *elements]

        return await self.run(command)

    async def rpushx(self, key: str, *elements: Any) -> int:
        """
        See https://redis.io/commands/rpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["RPUSHX", key, *elements]

        return await self.run(command)

    async def publish(self, channel: str, message: str) -> int:
        """
        See https://redis.io/commands/publish
        """

        command: List = ["PUBLISH", channel, message]

        return await self.run(command)

    async def eval(
        self,
        script: str,
        keys: Union[List[str], None] = None,
        args: Union[List, None] = None,
    ) -> Any:
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

        return await self.run(command)

    async def evalsha(
        self,
        sha1: str,
        keys: Union[List[str], None] = None,
        args: Union[List, None] = None,
    ) -> Any:
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

        return await self.run(command)

    async def dbsize(self) -> int:
        """
        See https://redis.io/commands/dbsize
        """

        command: List = ["DBSIZE"]

        return await self.run(command)

    async def flushall(self, mode: Union[Literal["ASYNC", "SYNC"], None] = None) -> str:
        """
        See https://redis.io/commands/flushall
        """

        command: List = ["FLUSHALL"]

        if mode:
            command.append(mode)

        return await self.run(command)

    async def flushdb(self, mode: Union[Literal["ASYNC", "SYNC"], None] = None) -> str:
        """
        See https://redis.io/commands/flushdb
        """

        command: List = ["FLUSHDB"]

        if mode:
            command.append(mode)

        return await self.run(command)

    async def time(self) -> Union[List[str], Dict[str, int]]:
        """
        See https://redis.io/commands/time

        :return: A Dict with the keys "seconds" and "microseconds" if self.format_return is True.
        """

        command: List = ["TIME"]

        raw: List[str] = await self.run(command)

        return format_server_time_return(raw) if self.format_return else raw

    async def sadd(self, key: str, *members: Any) -> int:
        """
        See https://redis.io/commands/sadd
        """

        if len(members) == 0:
            raise Exception("At least one member must be added.")

        command: List = ["SADD", key, *members]

        return await self.run(command)

    async def scard(self, key: str) -> int:
        """
        See https://redis.io/commands/scard
        """

        command: List = ["SCARD", key]

        return await self.run(command)

    async def sdiff(self, *keys: str) -> List[str]:
        """
        See https://redis.io/commands/sdiff
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SDIFF", *keys]

        return await self.run(command)

    async def sdiffstore(self, destination: str, *keys: str) -> int:
        """
        See https://redis.io/commands/sdiffstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SDIFFSTORE", destination, *keys]

        return await self.run(command)

    async def sinter(self, *keys: str) -> List[str]:
        """
        See https://redis.io/commands/sinter
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SINTER", *keys]

        return await self.run(command)

    async def sinterstore(self, destination: str, *keys: str) -> int:
        """
        See https://redis.io/commands/sinterstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SINTERSTORE", destination, *keys]

        return await self.run(command)

    async def sismember(self, key: str, member: Any) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/sismember

        :return: A bool if self.format_return is True.
        """

        command: List = ["SISMEMBER", key, member]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def smembers(self, key: str) -> List[str]:
        """
        See https://redis.io/commands/smembers
        """

        command: List = ["SMEMBERS", key]

        return await self.run(command)

    async def smove(
        self, source: str, destination: str, member: Any
    ) -> Union[Literal[1, 0], bool]:
        """
        See https://redis.io/commands/smove

        :return: A bool if self.format_return is True.
        """

        command: List = ["SMOVE", source, destination, member]

        raw: Literal[1, 0] = await self.run(command)

        return bool(raw) if self.format_return else raw

    async def spop(
        self, key: str, count: Union[int, None] = None
    ) -> Union[(Union[str, None]), List[str]]:
        """
        See https://redis.io/commands/spop

        :param count: defaults to 1 on the server side
        """

        command: List = ["SPOP", key]

        if count is not None:
            command.append(count)

        return await self.run(command)

    async def srandmember(
        self, key: str, count: Union[int, None] = None
    ) -> Union[(Union[str, None]), List[str]]:
        """
        See https://redis.io/commands/srandmember

        :param count: defaults to 1 on the server side
        """

        command: List = ["SRANDMEMBER", key]

        if count is not None:
            command.append(count)

        return await self.run(command)

    async def srem(self, key: str, *members: Any) -> int:
        """
        See https://redis.io/commands/srem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["SREM", key, *members]

        return await self.run(command)

    async def sscan(
        self,
        key: str,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> Union[List[Union[str, List[str]]], List[Union[int, List[str]]]]:
        """
        See https://redis.io/commands/sscan

        :param return_cursor: if set to False, it won't return the cursor
        :param match_pattern: replacement for "MATCH"

        :return: The cursor will be an integer if "format_return" is True.
        Only the List of elements will be returned if "return_cursor" is False.
        """

        command: List = ["SSCAN", key, cursor]

        if match_pattern is not None:
            command.extend(["MATCH", match_pattern])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        raw: List[Union[str, List[str]]] = await self.run(command)

        return [int(raw[0]), raw[1]] if self.format_return else raw


    async def sunion(self, *keys: str) -> List[str]:
        """
        See https://redis.io/commands/sunion
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SUNION", *keys]

        return await self.run(command)

    async def sunionstore(self, destination: str, *keys: str) -> int:
        """
        See https://redis.io/commands/sunionstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SUNIONSTORE", destination, *keys]

        return await self.run(command)

    async def zadd(
        self,
        key: str,
        score_member_pairs: Dict,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
        incr: bool = False,
    ) -> Union[int, (Union[str, None, float])]:
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

            raw: (Union[str, None]) = await self.run(command)

            return float(raw) if self.format_return and raw is not None else raw

        for name, score in score_member_pairs.items():
            command.extend([score, name])

        return await self.run(command)

    async def zcard(self, key: str) -> int:
        """
        See https://redis.io/commands/zcard
        """

        command: List = ["ZCARD", key]

        return await self.run(command)

    async def zcount(
        self, key: str, min_score: FloatMinMax, max_score: FloatMinMax
    ) -> int:
        """
        See https://redis.io/commands/zcount

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: List = ["ZCOUNT", key, min_score, max_score]

        return await self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be List[str].
    """

    async def zdiff(
        self, *keys: str, withscores: bool = False
    ) -> Union[SortedSetReturn, FormattedSortedSetReturn]:
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

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zdiffstore(self, destination: str, *keys: str) -> int:
        """
        See https://redis.io/commands/zdiffstore

        The number of keys is calculated automatically.
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZDIFFSTORE", destination, len(keys), *keys]

        return await self.run(command)

    async def zincrby(
        self, key: str, increment: float, member: str
    ) -> Union[str, float]:
        """
        See https://redis.io/commands/zincrby

        :return: A float if "format_return" is True.
        """

        command: List = ["ZINCRBY", key, increment, member]

        raw: str = await self.run(command)

        return float(raw) if self.format_return else raw

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be List[str].
    """

    async def zinter(
        self,
        *keys: str,
        weights: Union[List[float], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
        withscores: bool = False,
    ) -> Union[SortedSetReturn, FormattedSortedSetReturn]:
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

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zinterstore(
        self,
        destination: str,
        *keys: str,
        weights: Union[List[float], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
    ) -> int:
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

        return await self.run(command)

    async def zlexcount(self, key: str, min_score: str, max_score: str) -> int:
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

        return await self.run(command)

    async def zmscore(
        self, key: str, *members: str
    ) -> Union[List[Union[str, None]], List[Union[float, None]]]:
        """
        See https://redis.io/commands/zmscore

        :return: A List of float or None values if "format_return" is True.
        """

        if len(members) == 0:
            raise Exception("At least one member must be specified.")

        command: List = ["ZMSCORE", key, *members]

        raw: List[Union[str, None]] = await self.run(command)

        return format_float_list(raw) if self.format_return else raw

    async def zpopmax(
        self, key: str, count: Union[int, None] = None
    ) -> Union[SortedSetReturn, FormattedSortedSetReturn]:
        """
        See https://redis.io/commands/zpopmax

        :param count: defaults to 1 on the server side

        :return: A Dict of member-score pairs if "format_return" is True.
        """

        command: List = ["ZPOPMAX", key]

        if count is not None:
            command.append(count)

        raw: SortedSetReturn = await self.run(command)

        return format_sorted_set_return(raw) if self.format_return else raw

    async def zpopmin(
        self, key: str, count: Union[int, None] = None
    ) -> Union[SortedSetReturn, FormattedSortedSetReturn]:
        """
        See https://redis.io/commands/zpopmin

        :param count: defaults to 1 on the server side

        :return: A Dict of member-score pairs if "format_return" is True.
        """

        command: List = ["ZPOPMIN", key]

        if count is not None:
            command.append(count)

        raw: SortedSetReturn = await self.run(command)

        return format_sorted_set_return(raw) if self.format_return else raw

    async def zrandmember(
        self, key: str, count: Union[int, None] = None, withscores: bool = False
    ) -> Union[(Union[str, None]), (Union[SortedSetReturn, FormattedSortedSetReturn])]:
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

                raw: SortedSetReturn = await self.run(command)

                return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "with_scores" is True or not, its raw return type will be List[str].
    """

    async def zrange(
        self,
        key: str,
        start: FloatMinMax,
        stop: FloatMinMax,
        range_method: Union[Literal["BYSCORE", "BYLEX"], None] = None,
        rev: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
        withscores: bool = False,
    ) -> Union[SortedSetReturn, FormattedSortedSetReturn]:
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

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zrangebylex(
        self,
        key: str,
        min_score: str,
        max_score: str,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> List[Union[str, None]]:
        """
        See https://redis.io/commands/zrangebylex

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"
        """

        handle_zrangebylex_exceptions(min_score, max_score, limit_offset, limit_count)

        command: List = ["ZRANGEBYLEX", key, min_score, max_score]

        if limit_offset is not None:
            command.extend(["LIMIT", limit_offset, limit_count])

        return await self.run(command)

    """
    This has actually 3 return scenarios, but, 
    whether "withscores" is True or not, its raw return type will be List[str].
    """

    async def zrangebyscore(
        self,
        key: str,
        min_score: FloatMinMax,
        max_score: FloatMinMax,
        withscores: bool = False,
        offset: Union[int, None] = None,
        count: Union[int, None] = None,
    ) -> Union[SortedSetReturn, FormattedSortedSetReturn]:
        """
        See https://redis.io/commands/zrangebyscore

        If you need to use "-inf" and "+inf", please write them as strings.

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"

        :return: A Dict of member-score pairs if "withscores" and "format_return" are True.
        """

        if number_are_not_none(offset, count, number=1):
            raise Exception('Both "offset" and "count" must be specified.')

        command: List = ["ZRANGEBYSCORE", key, min_score, max_score]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if withscores:
            command.append("WITHSCORES")

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zrangestore(
        self,
        dst: str,
        src: str,
        min_score: FloatMinMax,
        max_score: FloatMinMax,
        range_method: Union[Literal["BYSCORE", "BYLEX"], None] = None,
        rev: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> int:
        """
        See https://redis.io/commands/zrangestore

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        handle_non_deprecated_zrange_exceptions(
            range_method, min_score, max_score, limit_offset, limit_count
        )

        command: List = ["ZRANGESTORE", dst, src, min_score, max_score]

        if range_method:
            command.append(range_method)

        if rev:
            command.append("REV")

        if limit_offset is not None:
            command.extend(["LIMIT", limit_offset, limit_count])

        return await self.run(command)

    async def zrank(self, key: str, member: str) -> Union[int, None]:
        """
        See https://redis.io/commands/zrank
        """

        command: List = ["ZRANK", key, member]

        return await self.run(command)

    async def zrem(self, key: str, *members: str) -> int:
        """
        See https://redis.io/commands/zrem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["ZREM", key, *members]

        return await self.run(command)

    async def zremrangebylex(self, key: str, min_score: str, max_score: str) -> int:
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

        return await self.run(command)

    async def zremrangebyrank(self, key: str, start: int, stop: int) -> int:
        """
        See https://redis.io/commands/zremrangebyrank
        """

        command: List = ["ZREMRANGEBYRANK", key, start, stop]

        return await self.run(command)

    async def zremrangebyscore(
        self, key: str, min_score: FloatMinMax, max_score: FloatMinMax
    ) -> int:
        """
        See https://redis.io/commands/zremrangebyscore\
        
        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: List = ["ZREMRANGEBYSCORE", key, min_score, max_score]

        return await self.run(command)

    """
    This has actually 3 return scenarios, but,
    whether "with_scores" is True or not, its raw return type will be List[str].
    """

    async def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> Union[SortedSetReturn, FormattedSortedSetReturn]:
        """
        See https://redis.io/commands/zrevrange

        :return: A Dict of member-score pairs if "withscores" and "format_return" are True.
        """

        command: List = ["ZREVRANGE", key, start, stop]

        if withscores:
            command.append("WITHSCORES")

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zrevrangebylex(
        self,
        key: str,
        max_score: str,
        min_score: str,
        offset: Union[int, None] = None,
        count: Union[int, None] = None,
    ) -> List[str]:
        """
        See https://redis.io/commands/zrevrangebylex

        :param min_score: replacement for "MIN"
        :param max_score: replacement for "MAX"
        """

        handle_zrangebylex_exceptions(min_score, max_score, offset, count)

        command: List = ["ZREVRANGEBYLEX", key, max_score, min_score]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return await self.run(command)

    """
    This has actually 3 return scenarios, but,
    whether "withscores" is True or not, its raw return type will be List[str].
    """

    async def zrevrangebyscore(
        self,
        key: str,
        max_score: FloatMinMax,
        min_score: FloatMinMax,
        withscores: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> Union[SortedSetReturn, FormattedSortedSetReturn]:
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

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zrevrank(self, key: str, member: str) -> Union[int, None]:
        """
        See https://redis.io/commands/zrevrank
        """

        command: List = ["ZREVRANK", key, member]

        return await self.run(command)

    async def zscan(
        self,
        key: str,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> Union[List[Union[str, SortedSetReturn]],List[Union[int, FormattedSortedSetReturn]]]:
        """
        See https://redis.io/commands/zscan

        :param return_cursor: if set to False, it won't return the cursor
        :param match_pattern: replacement for "MATCH"

        :return: The cursor will be an integer if "format_return" is True.
        Only a Dict of member-score pairs will be returned if "return_cursor" is False and "format_return" is True.
        """

        command: List = ["ZSCAN", key, cursor]

        if match_pattern is not None:
            command.extend(["MATCH", match_pattern])

        if count is not None:
            command.extend(["COUNT", count])

        raw: List[Union[int, SortedSetReturn]] = await self.run(command)

        return [int(raw[0]), format_sorted_set_return(raw[1])] if self.format_return else raw


    async def zscore(self, key: str, member: str) -> Union[str, None, float]:
        """
        See https://redis.io/commands/zscore

        :return: A float or None if "format_return" is True.
        """

        command: List = ["ZSCORE", key, member]

        raw: Union[str, None] = await self.run(command)

        return float(raw) if self.format_return and raw is not None else raw

    """
    This has actually 3 return scenarios, but,
    whether "withscores" is True or not, its raw return type will be List[str].
    """

    async def zunion(
        self,
        *keys: str,
        weights: Union[List[float], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
        withscores: bool = False,
    ) -> Union[SortedSetReturn, FormattedSortedSetReturn]:
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

            raw: SortedSetReturn = await self.run(command)

            return format_sorted_set_return(raw) if self.format_return else raw

        return await self.run(command)

    async def zunionstore(
        self,
        destination: str,
        *keys: str,
        weights: Union[List[float], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
    ) -> int:
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

        return await self.run(command)

    async def append(self, key: str, value: Any) -> int:
        """
        See https://redis.io/commands/append
        """

        command: List = ["APPEND", key, value]

        return await self.run(command)

    async def decr(self, key: str) -> int:
        """
        See https://redis.io/commands/decr
        """

        command: List = ["DECR", key]

        return await self.run(command)

    async def decrby(self, key: str, decrement: int) -> int:
        """
        See https://redis.io/commands/decrby
        """

        command: List = ["DECRBY", key, decrement]

        return await self.run(command)

    async def get(self, key: str) -> Union[str, None]:
        """
        See https://redis.io/commands/get
        """

        command: List = ["GET", key]

        return await self.run(command)

    async def getdel(self, key: str) -> Union[str, None]:
        """
        See https://redis.io/commands/getdel
        """

        command: List = ["GETDEL", key]

        return await self.run(command)

    async def getex(
        self,
        key: str,
        ex: Union[int, None] = None,
        px: Union[int, None] = None,
        exat: Union[int, None] = None,
        pxat: Union[int, None] = None,
        persist: Union[bool, None] = None,
    ) -> Union[str, None]:
        """
        See https://redis.io/commands/getex
        """

        if not number_are_not_none(ex, px, exat, pxat, persist, number=1):
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

        return await self.run(command)

    async def getrange(self, key: str, start: int, end: int) -> str:
        """
        See https://redis.io/commands/getrange
        """

        command: List = ["GETRANGE", key, start, end]

        return await self.run(command)

    async def getset(self, key: str, value: Any) -> Union[str, None]:
        """
        See https://redis.io/commands/getset
        """

        command: List = ["GETSET", key, value]

        return await self.run(command)

    async def incr(self, key: str) -> int:
        """
        See https://redis.io/commands/incr
        """

        command: List = ["INCR", key]

        return await self.run(command)

    async def incrby(self, key: str, increment: int) -> int:
        """
        See https://redis.io/commands/incrby
        """

        command: List = ["INCRBY", key, increment]

        return await self.run(command)

    async def incrbyfloat(self, key: str, increment: float) -> Union[str, float]:
        """
        See https://redis.io/commands/incrbyfloat

        :return: A float if "format_return" is True.
        """

        command: List = ["INCRBYFLOAT", key, increment]

        raw: str = await self.run(command)

        return float(raw) if self.format_return else raw

    async def mget(self, *keys: str) -> List[Union[str, None]]:
        """
        See https://redis.io/commands/mget
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["MGET", *keys]

        return await self.run(command)

    async def mset(self, key_value_pairs: Dict) -> Literal["OK"]:
        """
        See https://redis.io/commands/mset
        """

        command: List = ["MSET"]

        for key, value in key_value_pairs.items():
            command.extend([key, value])

        return await self.run(command)

    async def msetnx(self, key_value_pairs: Dict) -> Literal[1, 0]:
        """
        See https://redis.io/commands/msetnx
        """

        command: List = ["MSETNX"]

        for key, value in key_value_pairs.items():
            command.extend([key, value])

        return await self.run(command)

    async def psetex(self, key: str, milliseconds: int, value: Any) -> str:
        """
        See https://redis.io/commands/psetex
        """

        command: List = ["PSETEX", key, milliseconds, value]

        return await self.run(command)

    async def set(
        self,
        key: str,
        value: Any,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ex: Union[int, None] = None,
        px: Union[int, None] = None,
        exat: Union[int, None] = None,
        pxat: Union[int, None] = None,
        keepttl: bool = False,
    ) -> Union[str, None]:
        """
        See https://redis.io/commands/set
        """

        if nx and xx:
            raise Exception('"nx" and "xx" are mutually exclusive.')

        if not number_are_not_none(ex, px, exat, pxat, keepttl, number=1):
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

        return await self.run(command)

    async def setex(self, key: str, seconds: int, value: Any) -> str:
        """
        See https://redis.io/commands/setex
        """

        command: List = ["SETEX", key, seconds, value]

        return await self.run(command)

    async def setnx(self, key: str, value: Any) -> Literal[1, 0]:
        """
        See https://redis.io/commands/setnx
        """

        command: List = ["SETNX", key, value]

        return await self.run(command)

    async def setrange(self, key: str, offset: int, value: Any) -> int:
        """
        See https://redis.io/commands/setrange
        """

        command: List = ["SETRANGE", key, offset, value]

        return await self.run(command)

    async def strlen(self, key: str) -> int:
        """
        See https://redis.io/commands/strlen
        """

        command: List = ["STRLEN", key]

        return await self.run(command)

    async def substr(self, key: str, start: int, end: int) -> str:
        """
        See https://redis.io/commands/substr
        """

        command: List = ["SUBSTR", key, start, end]

        return await self.run(command)
    
    def pubsub(self) -> "PubSub":
        """
        See https://redis.io/commands/pubsub
        """

        return PubSub(client=self)
    
    def script(self) -> "Script":
        """
        See https://redis.io/commands/script
        """

        return Script(client=self)
    
# It doesn't inherit from "Redis" mainly because of the methods signatures.
class BitFieldCommands:
    def __init__(self, client: Commands, key: str):
        self.client = client
        self.command: List = ["BITFIELD", key]

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

    async def execute(self) -> List:
        return await self.client.run(command=self.command)


class BitFieldRO:
    def __init__(self, client: Commands, key: str):
        self.client = client
        self.command: List = ["BITFIELD_RO", key]

    def get(self, encoding: str, offset: BitFieldOffset):
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield_ro
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    async def execute(self) -> List:
        return await self.client.run(command=self.command)


class PubSub:
    def __init__(self, client: Commands):
        self.client = client

    async def channels(self, pattern: Union[str, None] = None) -> List[str]:
        """
        See https://redis.io/commands/pubsub-channels
        """

        command: List = ["PUBSUB", "CHANNELS"]

        if pattern is not None:
            command.append(pattern)

        return await self.client.run(command=command)

    async def numpat(self) -> int:
        """
        See https://redis.io/commands/pubsub-numpat
        """

        command: List = ["PUBSUB", "NUMPAT"]

        return await self.client.run(command=command)

    async def numsub(
        self, *channels: str
    ) -> Union[List[Union[str, int]], Dict[str, int]]:
        """
        See https://redis.io/commands/pubsub-numsub

        :return: A Dict with channel-number_of_subscribers pairs if "format_return" is True.
        """

        command: List = ["PUBSUB", "NUMSUB", *channels]

        raw: List[Union[str, int]] = await self.client.run(command)

        return format_pubsub_numsub_return(raw) if self.client.format_return else raw


class Script:
    def __init__(self, client: Commands):
        self.client = client

    async def exists(self, *sha1: str) -> Union[List[Literal[1, 0]], List[bool]]:
        """
        See https://redis.io/commands/script-exists

        :return: A List of bools if "format_return" is True.
        """

        if len(sha1) == 0:
            raise Exception("At least one sha1 digests must be provided.")

        command: List = ["SCRIPT", "EXISTS", *sha1]

        raw: List[Literal[1, 0]] = await self.client.run(command=command)

        return format_bool_list(raw) if self.client.format_return else raw

    async def flush(self, mode: Literal["ASYNC", "SYNC"]) -> str:
        """
        See https://redis.io/commands/script-flush
        """

        command: List = ["SCRIPT", "FLUSH"]

        if mode:
            command.append(mode)

        return await self.client.run(command=command)

    async def load(self, script: str) -> str:
        """
        See https://redis.io/commands/script-load
        """

        command: List = ["SCRIPT", "LOAD", script]

        return await self.client.run(command=command)


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
"""
