from upstash_py.http.execute import execute
from typing import ParamSpec, TypeVar, Callable

CommandParams = ParamSpec("CommandParams")
# At least for now, this will always be str.
CommandReturn = TypeVar("CommandReturn")


class Redis:
    def __init__(self, url: str, token: str, enable_telemetry: bool = False):
        self.url = url
        self.token = token
        self.enable_telemetry = enable_telemetry

    @staticmethod
    def command(func: Callable[CommandParams, CommandReturn]) -> Callable[CommandParams, CommandReturn]:
        def wrapper(*args: CommandParams.args, **kwargs: CommandParams.kwargs) -> CommandReturn:
            """
            Every function decorated with @command should return
            the Redis command that will be sent over the REST API
            """
            command = func(*args, **kwargs)
            # The first positional argument will always be the class instance, hence args[0] = self
            return execute(url=args[0].url, token=args[0].token, command=command)

        return wrapper

    @command
    def get(self, key: str) -> str:
        return f'get {key}'

    @command
    def set(self, key: str, value: str) -> str:
        return f'set {key} {value}'

