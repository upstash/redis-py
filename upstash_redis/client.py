from os import environ
from typing import Any, List, Literal, Optional, Type, Dict

from requests import Session

from upstash_redis.commands import Commands, PipelineCommands
from upstash_redis.format import cast_response
from upstash_redis.http import make_headers, sync_execute
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
    ):
        """
        Creates a new blocking Redis client.

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
        self._session = Session()

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
        self._session.close()

    def execute(self, command: List) -> RESTResultT:
        """
        Executes the given command.
        """

        res = sync_execute(
            session=self._session,
            url=self._url,
            headers=self._headers,
            encoding=self._rest_encoding,
            retries=self._rest_retries,
            retry_interval=self._rest_retry_interval,
            command=command,
        )

        return cast_response(command, res)

    def pipeline(self) -> "Pipeline":
        """
        Create a pipeline to send commands in batches
        """
        return Pipeline(
            url=self._url,
            token=self._token,
            rest_encoding=self._rest_encoding,
            rest_retries=self._rest_retries,
            rest_retry_interval=self._rest_retry_interval,
            allow_telemetry=self._allow_telemetry,
            headers=self._headers,
            session=self._session,
            multi_exec="pipeline"
        )

    def multi(self) -> "Pipeline":
        """
        Create a pipeline to send commands in batches as a transaction
        """
        return Pipeline(
            url=self._url,
            token=self._token,
            rest_encoding=self._rest_encoding,
            rest_retries=self._rest_retries,
            rest_retry_interval=self._rest_retry_interval,
            allow_telemetry=self._allow_telemetry,
            headers=self._headers,
            session=self._session,
            multi_exec="multi-exec"
        )


class Pipeline(PipelineCommands):

    def __init__(
        self,
        url: str,
        token: str,
        rest_encoding: Optional[Literal["base64"]] = "base64",
        rest_retries: int = 1,
        rest_retry_interval: float = 3,  # Seconds.
        allow_telemetry: bool = True,
        headers: Optional[Dict[str, str]] = None,
        session: Optional[Session] = None,
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

        self._url = url
        self._token = token

        self._allow_telemetry = allow_telemetry

        self._rest_encoding: Optional[Literal["base64"]] = rest_encoding
        self._rest_retries = rest_retries
        self._rest_retry_interval = rest_retry_interval

        self._headers = headers or make_headers(token, rest_encoding, allow_telemetry)
        self._session = session or Session()
        
        self._command_stack: List[List[str]] = []
        self._multi_exec = multi_exec

    def execute(self, command: List) -> "Pipeline":
        """
        Adds commnd to the command stack which will be sent as a batch
        later

        :param command: Command to execute
        """
        self._command_stack.append(command)
        return self

    def exec(self) -> List[RESTResultT]:
        """
        Executes the commands in the pipeline by sending them as a batch
        """
        url = f"{self._url}/{self._multi_exec}"
        res: List[RESTResultT] = sync_execute( # type: ignore[assignment]
            session=self._session,
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
