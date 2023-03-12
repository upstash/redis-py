from upstash_py.http.execute import execute
from upstash_py.schema import RESTResult
from upstash_py.config import config


class Redis:
    def __init__(
        self,
        url: str,
        token: str,
        enable_telemetry: bool = False,
        http_encoding: str | bool = config["HTTP_ENCODING"],
        http_retries: int = int(config["HTTP_RETRIES"]),
        http_retry_interval: int = int(config["HTTP_RETRY_INTERVAL"])
    ):
        self.url = url
        self.token = token
        self.enable_telemetry = enable_telemetry
        # If the encoding is set as "True", we default to config.
        self.http_encoding = config["HTTP_ENCODING"] if http_encoding is True else http_encoding
        self.http_retries = http_retries
        self.http_retry_interval = http_retry_interval

    def run(self, command: str) -> RESTResult:
        """
        Specify the http options and execute the command
        """

        return execute(
            url=self.url,
            token=self.token,
            encoding=self.http_encoding,
            retries=self.http_retries,
            retry_interval=self.http_retry_interval,
            command=command,
        )

    def get(self, key: str) -> str:
        """
        See https://redis.io/commands/get
        """

        command: str = f'get {key}'

        return self.run(command=command)

    def set(self, key: str, value: str) -> str:
        """
        See https://redis.io/commands/set
        """

        command: str = f'set {key} {value}'

        return self.run(command=command)

    def lpush(self, key: str, *elements: str) -> int:
        """
        See https://redis.io/commands/lpush
        """

        command: str = f'lpush {key}'

        for element in elements:
            command += " " + element

        return self.run(command=command)

    def lrange(self, key: str, start: int, stop: int) -> list:
        """
        See https://redis.io/commands/lpush
        """

        command: str = f'lrange {key} {start} {stop}'

        return self.run(command=command)
