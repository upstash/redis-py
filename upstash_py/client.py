from upstash_py.http.execute import execute
from upstash_py.schema import RESTResult


class Redis:
    def __init__(self, url: str, token: str, enable_telemetry: bool = False):
        self.url = url
        self.token = token
        self.enable_telemetry = enable_telemetry

    def run(self, command: str) -> RESTResult:
        """
        Specify the http options and execute the command
        """

        return execute(url=self.url, token=self.token, command=command)

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


redis = Redis(
    url="",
    token=""
)

