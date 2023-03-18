from upstash_py.http.execute import execute
from upstash_py.http.schema import RESTResult, RESTEncoding
from upstash_py.config import config
from aiohttp import ClientSession
from typing import Type, Any, Self, Literal


class Redis:
    def __init__(
        self,
        url: str,
        token: str,
        enable_telemetry: bool = False,
        rest_encoding: RESTEncoding = config["REST_ENCODING"],
        rest_retries: int = config["REST_RETRIES"],
        rest_retry_interval: int = config["REST_RETRY_INTERVAL"]
    ):
        self.url = url
        self.token = token
        self.enable_telemetry = enable_telemetry
        # If the encoding is set as "True", we default to config.
        self.rest_encoding = config["REST_ENCODING"] if rest_encoding is True else rest_encoding
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
            raise Exception("Both start and end must be specified")

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

    async def bitop(self, operation: Literal["AND", "OR", "XOR", "NOT"], dest_key: str, *src_keys: str) -> int:
        """
        See https://redis.io/commands/bitop
        """

        if operation == "NOT" and len(src_keys) > 1:
            raise Exception("NOT takes only one source key as argument.")

        command: list = ["BITOP", operation, dest_key]

        command.extend(src_keys)

        return await self.run(command=command)

    async def bitpos(self, key: str, bit: Literal[0, 1], start: int | None = None, end: int | None = None):
        """
        See https://redis.io/commands/bitpos
        """

        if start is None and end is not None:
            raise Exception("End is specified, but start is missing.")

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

    async def copy(self, source: str, destination: str, replace: bool) -> Literal[1, 0]:
        """
        See https://redis.io/commands/copy
        """

        command: list = ["COPY", source, destination]

        if replace:
            command.append("REPLACE")

        return await self.run(command=command)

    async def delete(self, key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/delete
        """

        command: list = ["DEL", key]

        if keys is not None:
            command.append(keys)

        return await self.run(command=command)

    async def exists(self, key: str, *keys: str) -> int:
        """
        See https://redis.io/commands/exists
        """

        command: list = ["EXISTS", key]

        if keys is not None:
            command.append(keys)

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

    async def rename(self, key: str, newkey: str) -> str:
        """
        See https://redis.io/commands/rename
        """

        command: list = ["RENAME", key, newkey]

        return await self.run(command=command)

    async def renamenx(self, key: str, newkey: str) -> Literal[1, 0]:
        """
        See https://redis.io/commands/renamenx
        """

        command: list = ["RENAMENX", key, newkey]

        return await self.run(command=command)

    async def scan(self):
        """
        See https://redis.io/commands/scan
        """

        command: list = []

        return await self.run(command=command)

    async def touch(self):
        """
        See https://redis.io/commands/touch
        """

        command: list = []

        return await self.run(command=command)

    async def ttl(self):
        """
        See https://redis.io/commands/ttl
        """

        command: list = []

        return await self.run(command=command)

    async def type(self):
        """
        See https://redis.io/commands/type
        """

        command: list = []

        return await self.run(command=command)

    async def unlink(self):
        """
        See https://redis.io/commands/unlink
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


# str allows for "#" syntax.
BitFieldOffset = str | int


# We don't inherit from "Redis" mainly because of the methods signatures
class BitFieldCommands:
    def __init__(self, client: Redis, key: str):
        self.client = client
        self.command: list = ["BITFIELD", key]

    def get(self, encoding: str, offset: BitFieldOffset) -> Self:
        """
        Returns the specified bit field.
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    def set(self, encoding: str, offset: BitFieldOffset, value: int) -> Self:
        """
        Set the specified bit field and returns its old value.
        """

        _command = ["SET", encoding, offset, value]
        self.command.extend(_command)

        return self

    def incrby(self, encoding: str, offset: BitFieldOffset, increment: int) -> Self:
        """
        Increments or decrements (if a negative increment is given) the specified bit field and returns the new value.
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
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    async def execute(self) -> RESTResult:
        return await self.client.run(command=self.command)
