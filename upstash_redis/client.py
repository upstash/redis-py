from os import environ
from typing import Any, List, Literal, Optional, Type, Dict, Callable

from upstash_redis.commands import (
    Commands,
    PipelineCommands,
    JsonCommands,
    PipelineJsonCommands,
)
from upstash_redis.format import cast_response
from upstash_redis.http import make_headers, SyncHttpClient
from upstash_redis.typing import RESTResultT


class Redis(Commands):
    """
    A Redis client that uses the Upstash REST API.

    Blocking example:

    ```python
    from upstash_redis import Redis

    redis = Redis.from_env()

    redis.set("key", "value")
    redis.get("key")
    ```

    To use the async client, use `upstash_redis.asyncio.Redis`.
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
        Creates a new blocking Redis client.

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
        self._read_your_writes = read_your_writes
        self._sync_token = ""

        # self._allow_telemetry and self._headers need to stay in this class
        # for compatibility with the ratelimit library.
        self._allow_telemetry = allow_telemetry
        self._headers = make_headers(token, rest_encoding, allow_telemetry)

        self._json = JsonCommands(self)
        self._http = SyncHttpClient(
            encoding=rest_encoding,
            retries=rest_retries,
            retry_interval=rest_retry_interval,
            sync_token_cb=self._update_sync_token,
        )

    @property
    def json(self) -> JsonCommands:
        return self._json

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

    def __enter__(self) -> "Redis":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        self.close()

    def close(self):
        """
        Closes the resources associated with the client.
        """
        self._http.close()

    def _update_sync_token(self, new_token: str):
        if self._read_your_writes:
            self._sync_token = new_token

    def _maybe_set_sync_token_header(self, headers: Dict[str, str]):
        if self._read_your_writes:
            headers["Upstash-Sync-Token"] = self._sync_token

    def execute(self, command: List) -> RESTResultT:
        """
        Executes the given command.
        """
        self._maybe_set_sync_token_header(self._headers)
        res = self._http.execute(
            url=self._url,
            headers=self._headers,
            command=command,
        )
        return cast_response(command, res)

    def pipeline(self) -> "Pipeline":
        """
        Create a pipeline to send commands in batches
        """
        return Pipeline(
            url=self._url,
            headers=self._headers,
            http=self._http,
            multi_exec="pipeline",
            set_sync_token_header_fn=self._maybe_set_sync_token_header,
        )

    def multi(self) -> "Pipeline":
        """
        Create a pipeline to send commands in batches as a transaction
        """
        return Pipeline(
            url=self._url,
            headers=self._headers,
            http=self._http,
            multi_exec="multi-exec",
            set_sync_token_header_fn=self._maybe_set_sync_token_header,
        )


class Pipeline(PipelineCommands):
    def __init__(
        self,
        url: str,
        headers: Dict[str, str],
        http: SyncHttpClient,
        multi_exec: Literal["multi-exec", "pipeline"],
        set_sync_token_header_fn: Callable[[Dict[str, str]], None],
    ):
        self._url = f"{url}/{multi_exec}"
        self._headers = headers
        self._http = http

        self._json = PipelineJsonCommands(self)
        self._command_stack: List[List[str]] = []

        self._set_sync_token_header_fn = set_sync_token_header_fn

    @property
    def json(self) -> PipelineJsonCommands:
        return self._json

    def execute(self, command: List) -> "Pipeline":
        """
        Adds command to the command stack which will be sent as a batch
        later

        :param command: Command to execute
        """
        self._command_stack.append(command)
        return self

    def exec(self) -> List[RESTResultT]:
        """
        Executes the commands in the pipeline by sending them as a batch
        """
        self._set_sync_token_header_fn(self._headers)

        res: List[RESTResultT] = self._http.execute(  # type: ignore[assignment]
            url=self._url,
            headers=self._headers,
            command=self._command_stack,
            from_pipeline=True,
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

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        self.reset()
