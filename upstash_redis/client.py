from os import environ
from typing import Any, List, Type, Union

from requests import Session

from upstash_redis.commands.commands import Commands
from upstash_redis.config import (
    ALLOW_TELEMETRY,
    FORMAT_RETURN,
    REST_ENCODING,
    REST_RETRIES,
    REST_RETRY_INTERVAL,
)
from upstash_redis.http import make_headers, sync_execute
from upstash_redis.schema.http import RESTEncoding, RESTResult
from upstash_redis.utils.format import FormattedResponse


class Redis(FormattedResponse, Commands):
    def __init__(
        self,
        url: str,
        token: str,
        rest_encoding: RESTEncoding = REST_ENCODING,
        rest_retries: int = REST_RETRIES,
        rest_retry_interval: int = REST_RETRY_INTERVAL,  # Seconds.
        format_return: bool = FORMAT_RETURN,
        allow_telemetry: bool = ALLOW_TELEMETRY,
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

        self.FORMATTERS = self.__class__.FORMATTERS

        self._headers = make_headers(token, rest_encoding, allow_telemetry)
        self._session = Session()

    def __enter__(self):
        """
        Enter the sync context.
        """
        if self._session is None:
            self._session = Session()

        # It needs to return the session object because it will be used in "sync with" statements.
        return self

    def __exit__(
        self,
        exc_type: Union[Type[BaseException], None],
        exc_val: Union[BaseException, None],
        exc_tb: Any,
    ) -> None:
        """
        Exit the sync context.
        """
        if self._session:
            self._session.close()

    @classmethod
    def from_env(
        cls,
        rest_encoding: RESTEncoding = REST_ENCODING,
        rest_retries: int = REST_RETRIES,
        rest_retry_interval: int = REST_RETRY_INTERVAL,
        format_return: bool = FORMAT_RETURN,
        allow_telemetry: bool = ALLOW_TELEMETRY,
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
        )

    def close(self):
        """
        Closes the session.
        """
        if self._session:
            self._session.close()

    def run(self, command: List) -> RESTResult:
        """
        Specify the http options and execute the command.
        """

        res = sync_execute(
            session=self._session,
            url=self.url,
            headers=self._headers,
            encoding=self.rest_encoding,
            retries=self.rest_retries,
            retry_interval=self.rest_retry_interval,
            command=command,
        )

        main_command = command[0]
        if len(command) > 1 and (main_command == "PUBSUB" or main_command == "SCRIPT"):
            main_command = f"{main_command} {command[1]}"

        if (
            self.format_return
            or main_command == "HSCAN"
            or main_command == "SMEMBERS"
            or main_command == "SDIFF"
            or main_command == "SINTER"
            or main_command == "SSCAN"
            or main_command == "SUNION"
            or main_command == "ZSCAN"
        ) and (main_command in self.FORMATTERS):
            return self.FORMATTERS[main_command](res, command)

        return res
