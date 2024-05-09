from os import environ
from typing import Any, List, Literal, Optional, Type

from aiohttp import ClientSession

from upstash_redis.commands import AsyncCommands
from upstash_redis.format import cast_response
from upstash_redis.http import async_execute, make_headers
from upstash_redis.typing import RESTResultT


class Redis(AsyncCommands):
    """
    A Redis client that uses the Upstash REST API.

    ```python
    from upstash_redis.asyncio import Redis

    redis = Redis.from_env()

    await redis.set("key", "value")
    await redis.get("key")
    ```

    To use the blocking client, use `upstash_redis.Redis`.
    """

    def __init__(
        self,
        url: str,
        token: str,
        rest_encoding: Optional[Literal["base64"]] = "base64",
        rest_retries: int = 1,
        rest_retry_interval: float = 3,  # Seconds.
        allow_telemetry: bool = True,
    ):
        """
        Creates a new async Redis client.

        :param url: UPSTASH_REDIS_REST_URL in the console
        :param token: UPSTASH_REDIS_REST_TOKEN in the console
        :param rest_encoding: the encoding that can be used by the REST API to parse the response before sending it
        :param rest_retries: how many times an HTTP request will be retried if it fails
        :param rest_retry_interval: how many seconds will be waited between each retry
        :param allow_telemetry: whether anonymous telemetry can be collected
        """

        self._url = url
        self._token = token

        self._allow_telemetry = allow_telemetry

        self._rest_encoding: Optional[Literal["base64"]] = rest_encoding
        self._rest_retries = rest_retries
        self._rest_retry_interval = rest_retry_interval

        self._headers = make_headers(token, rest_encoding, allow_telemetry)
        self._context_manager: Optional[_SessionContextManager] = None

    @classmethod
    def from_env(
        cls,
        rest_encoding: Optional[Literal["base64"]] = "base64",
        rest_retries: int = 1,
        rest_retry_interval: float = 3,
        allow_telemetry: bool = True,
    ):
        """
        Load the credentials from environment.

        :param rest_encoding: the encoding that can be used by the REST API to parse the response before sending it
        :param rest_retries: how many times an HTTP request will be retried if it fails
        :param rest_retry_interval: how many seconds will be waited between each retry
        :param allow_telemetry: whether anonymous telemetry can be collected
        """

        return cls(
            environ["UPSTASH_REDIS_REST_URL"],
            environ["UPSTASH_REDIS_REST_TOKEN"],
            rest_encoding,
            rest_retries,
            rest_retry_interval,
            allow_telemetry,
        )

    async def __aenter__(self) -> "Redis":
        self._context_manager = _SessionContextManager(
            ClientSession(), close_session=False
        )
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """
        Closes the resources associated with the client.
        """
        if self._context_manager:
            await self._context_manager.close_session()
            self._context_manager = None

    async def execute(self, command: List) -> RESTResultT:
        """
        Executes the given command.
        """
        context_manager = self._context_manager
        if not context_manager:
            context_manager = _SessionContextManager(
                ClientSession(), close_session=True
            )

        async with context_manager:
            res = await async_execute(
                session=context_manager.session,
                url=self._url,
                headers=self._headers,
                encoding=self._rest_encoding,
                retries=self._rest_retries,
                retry_interval=self._rest_retry_interval,
                command=command,
            )

        return cast_response(command, res)

    def pipeline(self):
        return AsyncPipeline(
            url=self._url,
            token=self._token,
            rest_encoding=self._rest_encoding,
            rest_retries=self._rest_retries,
            rest_retry_interval=self._rest_retry_interval,
            allow_telemetry=self._allow_telemetry,
            multi_exec="pipeline"
        )

    def multi(self):
        return AsyncPipeline(
            url=self._url,
            token=self._token,
            rest_encoding=self._rest_encoding,
            rest_retries=self._rest_retries,
            rest_retry_interval=self._rest_retry_interval,
            allow_telemetry=self._allow_telemetry,
            multi_exec="multi-exec"
        )


class AsyncPipeline(Redis):

    def __init__(
        self,
        url: str,
        token: str,
        rest_encoding: Optional[Literal["base64"]] = "base64",
        rest_retries: int = 1,
        rest_retry_interval: float = 3,  # Seconds.
        allow_telemetry: bool = True,
        multi_exec: Literal["multi-exec", "pipeline"] = "pipeline"
    ):
        """
        Creates a new blocking Redis client.

        :param url: UPSTASH_REDIS_REST_URL in the console
        :param token: UPSTASH_REDIS_REST_TOKEN in the console
        :param rest_encoding: the encoding that can be used by the REST API to parse the response before sending it
        :param rest_retries: how many times an HTTP request will be retried if it fails
        :param rest_retry_interval: how many seconds will be waited between each retry
        :param allow_telemetry: whether anonymous telemetry can be collected
        :param headers: request headers
        :param session: A Requests session
        :param miltiexec: Whether multi execution (transaction) or pipelining is to be used
        """
        super().__init__(
            url=url,
            token=token,
            rest_encoding=rest_encoding,
            rest_retries=rest_retries,
            rest_retry_interval=rest_retry_interval,
            allow_telemetry=allow_telemetry,
        )
        
        self._command_stack: List[List[str]] = []
        self._multi_exec = multi_exec

    def execute(self, command: List) -> None:
        self._command_stack.append(command)

    async def exec(self) -> List[RESTResultT]:

        url = f"{self._url}/{self._multi_exec}"
        
        context_manager = self._context_manager
        if not context_manager:
            context_manager = _SessionContextManager(
                ClientSession(), close_session=True
            )

        async with context_manager:
            res = await async_execute(
                session=context_manager.session,
                url=url,
                headers=self._headers,
                encoding=self._rest_encoding,
                retries=self._rest_retries,
                retry_interval=self._rest_retry_interval,
                command=self._command_stack,
                from_pipeline=True
            )

        response = [
            cast_response(command, response)
            for command, response in zip(self._command_stack, res)
        ]
        self._command_stack = []
        return response
    
    def pipeline(self):
        raise NotImplementedError("A pipeline can not be created from a pipeline!")
    
    def multi(self):
        raise NotImplementedError("A pipeline can not be created from a pipeline!")


class _SessionContextManager:
    """
    Allows a session to be re-used in multiple async with
    blocks when the `close_session` is False.

    Main use case is to re-use sessions in multiple HTTP
    requests when the client is used in an async with block.

    The logic around the places in which we use this class is
    required so that the same client can be re-used even in
    different event loops, one after another.
    """

    def __init__(self, session: ClientSession, close_session: bool) -> None:
        self.session = session
        self._close_session = close_session

    async def close_session(self) -> None:
        await self.session.close()

    async def __aenter__(self) -> ClientSession:
        return self.session

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        if self._close_session:
            await self.close_session()
