from upstash_py.http.execute import execute
from upstash_py.schema import RESTResult, RESTEncoding
from upstash_py.config import config
from aiohttp import ClientSession
from typing import Type, Any


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

    async def bitcount(self, key: str, start: int = 0, end: int = -1) -> int:
        """
        See https://redis.io/commands/bitcount
        """
        command: list = ["BITCOUNT", key, start, end]

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
