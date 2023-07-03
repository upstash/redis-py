from typing import Protocol, Union, Awaitable, Any

ResponseType = Union[Awaitable, Any]


class CommandsProtocol(Protocol):
    def run(self, *args, **options):
        ...
