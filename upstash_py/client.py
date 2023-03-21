from upstash_py.http.execute import execute
from upstash_py.schema.http import RESTResult, RESTEncoding
from upstash_py.config import config
from aiohttp import ClientSession
from typing import Type, Any, Self, Literal
from schema.commands.parameters import BitFieldOffset, GeoMember


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

        # If the encoding is set as "True", we default to config.
        self.rest_encoding = config["REST_ENCODING"] if rest_encoding else rest_encoding
        self.rest_retries = rest_retries
        self.rest_retry_interval = rest_retry_interval

        self._session: ClientSession | None = None

    async def __aenter__(self) -> ClientSession:
        """
        Enter the async context.
        """

        self._session = ClientSession()
        # We need to return the session object because it will be used in "async with" statements.
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
                Both "START" and "END" must be specified.
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
                "NOT" takes only one source key as argument.
                """
            )

        command: list = ["BITOP", operation, destination_key]

        command.extend(source_keys)

        return await self.run(command=command)

    async def bitpos(self, key: str, bit: Literal[0, 1], start: int | None = None, end: int | None = None):
        """
        See https://redis.io/commands/bitpos
        """

        if start is None and end is not None:
            raise Exception(
                """
                "END" is specified, but "START" is missing.
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

    async def setbit(self, key: str, offset: int, value: int) -> int:
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

    async def copy(self, source: str, destination: str, replace: bool | None = None) -> Literal[1, 0]:
        """
        See https://redis.io/commands/copy
        """

        command: list = ["COPY", source, destination]

        if replace:
            command.append("REPLACE")

        return await self.run(command=command)

    async def delete(self, *keys: str) -> int:
        """
        See https://redis.io/commands/del
        """

        command: list = ["DEL"]

        command.extend(keys)

        return await self.run(command=command)

    async def exists(self, *keys: str) -> int:
        """
        See https://redis.io/commands/exists
        """

        command: list = ["EXISTS"]

        command.extend(keys)

        return await self.run(command=command)

    async def expire(self, key: str, seconds: int) -> Literal[1, 0]:
        """
        See https://redis.io/commands/expire
        """

        command: list = ["EXPIRE", key, seconds]

        return await self.run(command=command)

    async def expireat(self, key: str, unix_time_seconds: int) -> Literal[1, 0]:
        """
        See https://redis.io/commands/expireat
        """

        command: list = ["EXPIREAT", key, unix_time_seconds]

        return await self.run(command=command)

    async def keys(self, pattern: str) -> list[str]:
        """
        See https://redis.io/commands/keys
        """

        command: list = ["KEYS", pattern]

        return await self.run(command=command)

    async def persist(self, key: str) -> Literal[1, 0]:
        """
        See https://redis.io/commands/persist
        """

        command: list = ["PERSIST", key]

        return await self.run(command=command)

    async def pexpire(self, key: str, milliseconds: int) -> Literal[1, 0]:
        """
        See https://redis.io/commands/pexpire
        """

        command: list = ["PEXPIRE", key, milliseconds]

        return await self.run(command=command)

    async def pexpireat(self, key: str, unix_time_milliseconds: int) -> Literal[1, 0]:
        """
        See https://redis.io/commands/pexpireat
        """

        command: list = ["EXPIREAT", key, unix_time_milliseconds]

        return await self.run(command=command)

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

    async def renamenx(self, key: str, new_key: str) -> Literal[1, 0]:
        """
        See https://redis.io/commands/renamenx
        """

        command: list = ["RENAMENX", key, new_key]

        return await self.run(command=command)

    async def scan(
        self,
        cursor: int,
        match: str,
        count: int,
        scan_type: str,
        return_cursor: bool = True
    ) -> list[int, list] | list:
        """
        See https://redis.io/commands/scan

        "TYPE" was replaced with "scan_type".

        If "return_cursor" is False, it won't return the cursor.
        """

        command: list = ["SCAN", cursor]

        if match is not None:
            command.extend(["MATCH", match])

        if count is not None:
            command.extend(["COUNT", count])

        if scan_type is not None:
            command.extend(["TYPE", scan_type])

        result = await self.run(command=command)

        # The result is composed of the new cursor and the array of elements.
        return result if return_cursor else result[1]

    async def touch(self, *keys: str) -> int:
        """
        See https://redis.io/commands/touch
        """

        command: list = ["TOUCH"]

        command.extend(keys)

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

        command: list = ["UNLINK"]

        command.extend(keys)

        return await self.run(command=command)

    async def geoadd(
        self,
        key: str,
        nx: bool | None = None,
        xx: bool | None = None,
        ch: bool | None = None,
        *members: GeoMember
    ) -> int:
        """
        See https://redis.io/commands/geoadd
        """

        if nx is not None and xx is not None:
            raise Exception(
                """
                "XX" and "NX" are mutually exclusive.
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
        unit: Literal["M", "KM", "FT", "MI"] | None = None
    ) -> str | None:  # Can be a double represented as string.
        """
        See https://redis.io/commands/geodist

        The measuring unit can be passed with "unit".
        """

        command: list = ["GEODIST", key, first_member, second_member]

        if unit:
            command.append(unit)

        return await self.run(command=command)

    async def geohash(self, key: str, *members: str) -> list[str]:
        """
        See https://redis.io/commands/geohash
        """

        command: list = ["GEOHASH", key]

        command.extend(members)

        return await self.run(command=command)

    async def geopos(
        self,
        key: str,
        member: str,
        *members: str,
    ) -> list[str | None] | list[dict[str, float] | None]:
        """
        See https://redis.io/commands/geopos

        If "format_return" is True, it will return the result as a dict.
        """

        command: list = ["GEOPOS", key, member]

        if members is not None:
            command.extend(members)

        result = await self.run(command=command)

        return [
            {
                "longitude": float(member[0]),
                "latitude": float(member[1])
                # If the member doesn't exist, GEOPOS will return nil.
            } if isinstance(result, list) else None

            for member in result
        ] if self.format_return else result

    async def georadius(
        self,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"] | None = None,
        with_coordinates: bool | None = None,
        with_distance: bool | None = None,
        with_hash: bool | None = None,
        count: int | None = None,
        count_any: bool | None = None,
        sort: Literal["ASC", "DESC"] | None = None,
        store_as: str | None = None,
        store_distance_as: str | None = None
    ) -> list[str] | list[dict[str, float | int]]:
        """
        See https://redis.io/commands/georadius

        The measuring unit can be passed with "unit".

        "ANY" was replaced with "count_any".

        "ASC" and "DESC" are written as sort.

        "[STORE and STORE_DIST] key" are written as "store_as" and "store_distance_as".
        """

        if not self.allow_deprecated:
            raise Exception(
                """
                As of Redis version 6.2.0, this command is regarded as deprecated.
                It can be replaced by "GEOSEARCH" and "GEOSEARCHSTORE" with the "BYRADIUS" argument.
                
                Source: https://redis.io/commands/georadius
                """
            )

        if count_any is not None and count is None:
            raise Exception(
                """
                "ANY" can only be used together with "COUNT".
                """
            )

        command: list = ["GEORADIUS", longitude, latitude, radius]

        if unit:
            command.append(unit)

        if with_coordinates:
            command.append("WITHCOORD")

        if with_distance:
            command.append("WITHDIST")

        if with_hash:
            command.append("WITHHASH")

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

        result = await self.run(command=command)

        if self.format_return:
            formatted_result: list[dict[str, float | int]] = []

            for member in result:
                formatted_member: dict[str, float | int] = {
                    "member": member[0],
                }

    async def georadius_ro(self):
        """
        See https://redis.io/commands/georadius_ro
        """

        command: list = []

        return await self.run(command=command)

    async def georadiusbymember(self):
        """
        See https://redis.io/commands/georadiusbymember
        """

        command: list = []

        return await self.run(command=command)

    async def georadiusbymember_ro(self):
        """
        See https://redis.io/commands/georadiusbymember_ro
        """

        command: list = []

        return await self.run(command=command)

    async def geosearch(self):
        """
        See https://redis.io/commands/geosearch
        """

        command: list = []

        return await self.run(command=command)

    async def geosearchstore(self):
        """
        See https://redis.io/commands/geosearchstore
        """

        command: list = []

        return await self.run(command=command)

    async def get(self, key: str) -> str:
        """
        See https://redis.io/commands/get
        """

        command: list = ["GET", key]

        return await self.run(command=command)

    async def set(self, key: str, value: str) -> str:
        """
        See https://redis.io/commands/set
        """

        command: list = ["SET", key, value]

        return await self.run(command=command)

    async def lpush(self, key: str, *elements: str) -> int:
        """
        See https://redis.io/commands/lpush
        """

        command: list = ["LPUSH", key]

        command.extend(elements)

        return await self.run(command=command)

    async def lrange(self, key: str, start: int, stop: int) -> list:
        """
        See https://redis.io/commands/lpush
        """

        command: list = ["LRANGE", key, start, stop]

        return await self.run(command=command)


# We don't inherit from "Redis" mainly because of the methods signatures
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
