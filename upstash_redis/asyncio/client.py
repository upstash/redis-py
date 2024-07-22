from os import environ
from typing import Any, List, Literal, Optional, Type, Dict, Callable

from aiohttp import ClientSession

from upstash_redis.commands import AsyncCommands, PipelineCommands
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
        read_your_writes: bool = True,
    ):
        """
        Creates a new async Redis client.

        :param url: UPSTASH_REDIS_REST_URL in the console
        :param token: UPSTASH_REDIS_REST_TOKEN in the console
        :param rest_encoding: the encoding that can be used by the REST API to parse the response before sending it
        :param rest_retries: how many times an HTTP request will be retried if it fails
        :param rest_retry_interval: how many seconds will be waited between each retry
        :param allow_telemetry: whether anonymous telemetry can be collected
        :param read_your_writes: when enabled, any subsequent commands issued by this client are guaranteed to observe
            the effects of all earlier writes submitted by the same client.
        """

        self._url = url
        self._token = token

        self._allow_telemetry = allow_telemetry

        self._rest_encoding: Optional[Literal["base64"]] = rest_encoding
        self._rest_retries = rest_retries
        self._rest_retry_interval = rest_retry_interval

        self._read_your_writes = read_your_writes
        self._sync_token = ""

        self._headers = make_headers(token, rest_encoding, allow_telemetry)
        self._context_manager: Optional[_SessionContextManager] = None

    @classmethod
    def from_env(
        cls,
        rest_encoding: Optional[Literal["base64"]] = "base64",
        rest_retries: int = 1,
        rest_retry_interval: float = 3,
        allow_telemetry: bool = True,
        read_your_writes: bool = True,
    ):
        """
        Load the credentials from environment.

        :param rest_encoding: the encoding that can be used by the REST API to parse the response before sending it
        :param rest_retries: how many times an HTTP request will be retried if it fails
        :param rest_retry_interval: how many seconds will be waited between each retry
        :param allow_telemetry: whether anonymous telemetry can be collected
        :param read_your_writes: when enabled, any subsequent commands issued by this client are guaranteed to observe
            the effects of all earlier writes submitted by the same client.
        """

        return cls(
            environ["UPSTASH_REDIS_REST_URL"],
            environ["UPSTASH_REDIS_REST_TOKEN"],
            rest_encoding,
            rest_retries,
            rest_retry_interval,
            allow_telemetry,
            read_your_writes,
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

    def _update_sync_token(self, new_token: str):
        if self._read_your_writes:
            self._sync_token = new_token

    def _maybe_set_sync_token_header(self, headers: Dict[str, str]):
        if self._read_your_writes:
            headers["Upstash-Sync-Token"] = self._sync_token

    async def execute(self, command: List) -> RESTResultT:
        """
        Executes the given command.
        """
        context_manager = self._context_manager
        if not context_manager:
            context_manager = _SessionContextManager(
                ClientSession(), close_session=True
            )

        self._maybe_set_sync_token_header(self._headers)
        async with context_manager:
            res = await async_execute(
                session=context_manager.session,
                url=self._url,
                headers=self._headers,
                encoding=self._rest_encoding,
                retries=self._rest_retries,
                retry_interval=self._rest_retry_interval,
                command=command,
                sync_token_cb=self._update_sync_token,
            )

        return cast_response(command, res)

    def pipeline(self) -> "AsyncPipeline":
        """
        Create a pipeline to send commands in batches
        """
        return AsyncPipeline(
            url=self._url,
            token=self._token,
            rest_encoding=self._rest_encoding,
            rest_retries=self._rest_retries,
            rest_retry_interval=self._rest_retry_interval,
            allow_telemetry=self._allow_telemetry,
            headers=self._headers,
            context_manager=self._context_manager,
            multi_exec="pipeline",
            set_sync_token_header_fn=self._maybe_set_sync_token_header,
            sync_token_cb=self._update_sync_token,
        )

    def multi(self) -> "AsyncPipeline":
        """
        Create a pipeline to send commands in batches as a transaction
        """
        return AsyncPipeline(
            url=self._url,
            token=self._token,
            rest_encoding=self._rest_encoding,
            rest_retries=self._rest_retries,
            rest_retry_interval=self._rest_retry_interval,
            allow_telemetry=self._allow_telemetry,
            headers=self._headers,
            context_manager=self._context_manager,
            multi_exec="multi-exec",
            set_sync_token_header_fn=self._maybe_set_sync_token_header,
            sync_token_cb=self._update_sync_token,
        )


class AsyncPipeline(PipelineCommands):
    def __init__(
        self,
        url: str,
        token: str,
        rest_encoding: Optional[Literal["base64"]] = "base64",
        rest_retries: int = 1,
        rest_retry_interval: float = 3,  # Seconds.
        allow_telemetry: bool = True,
        context_manager: Optional["_SessionContextManager"] = None,
        headers: Optional[Dict[str, str]] = None,
        multi_exec: Literal["multi-exec", "pipeline"] = "pipeline",
        set_sync_token_header_fn: Optional[Callable[[Dict[str, str]], None]] = None,
        sync_token_cb: Optional[Callable[[str], None]] = None,
    ):
        """
        Creates a new blocking Redis client.

        :param url: UPSTASH_REDIS_REST_URL in the console
        :param token: UPSTASH_REDIS_REST_TOKEN in the console
        :param rest_encoding: the encoding that can be used by the REST API to parse the response before sending it
        :param rest_retries: how many times an HTTP request will be retried if it fails
        :param rest_retry_interval: how many seconds will be waited between each retry
        :param allow_telemetry: whether anonymous telemetry can be collected
        :param context_manager: context manager
        :param headers: request headers
        :param multiexec: Whether multi execution (transaction) or pipelining is to be used
        :param set_sync_token_header_fn: Function to set the Upstash-Sync-Token header
        :param sync_token_cb: Function to call when a new Upstash-Sync-Token response is received
        """

        self._url = url
        self._token = token

        self._allow_telemetry = allow_telemetry

        self._rest_encoding: Optional[Literal["base64"]] = rest_encoding
        self._rest_retries = rest_retries
        self._rest_retry_interval = rest_retry_interval

        self._headers = headers or make_headers(token, rest_encoding, allow_telemetry)
        self._context_manager = context_manager or _SessionContextManager(
            ClientSession(), close_session=True
        )

        self._command_stack: List[List[str]] = []
        self._multi_exec = multi_exec

        self._set_sync_token_header_fn = set_sync_token_header_fn
        self._sync_token_cb = sync_token_cb

    def execute(self, command: List) -> "AsyncPipeline":  # type: ignore[override]
        """
        Adds commnd to the command stack which will be sent as a batch
        later

        :param command: Command to execute
        """
        self._command_stack.append(command)
        return self

    async def exec(self) -> List[RESTResultT]:
        """
        Executes the commands in the pipeline by sending them as a batch
        """
        url = f"{self._url}/{self._multi_exec}"

        if self._set_sync_token_header_fn:
            self._set_sync_token_header_fn(self._headers)

        context_manager = self._context_manager
        async with context_manager:
            res: List[RESTResultT] = await async_execute(  # type: ignore[assignment]
                session=context_manager.session,
                url=url,
                headers=self._headers,
                encoding=self._rest_encoding,
                retries=self._rest_retries,
                retry_interval=self._rest_retry_interval,
                command=self._command_stack,
                from_pipeline=True,
                sync_token_cb=self._sync_token_cb,
            )

        response = [
            cast_response(command, response)
            for command, response in zip(self._command_stack, res)
        ]
        self.reset()
        return response

    def reset(self):
        """
        Resets the commands in the pipeline
        """
        self._command_stack = []

    async def __aenter__(self) -> "AsyncPipeline":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        self.reset()


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
