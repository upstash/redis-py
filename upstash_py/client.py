from upstash_py.http.execute import execute
from upstash_py.schema import RESTResponse


class Redis:
    def __init__(self, url: str, token: str, enable_telemetry: bool = False):
        self.url = url
        self.token = token
        self.enable_telemetry = enable_telemetry

    def run(self, command: str) -> RESTResponse:
        return execute(url=self.url, token=self.token, command=command)

    def get(self, key: str) -> str:
        command = f'get {key}'
        return self.run(command=command)

    def set(self, key: str, value: str) -> str:
        command = f'set {key} {value}'
        return self.run(command=command)

