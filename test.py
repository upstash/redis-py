from abc import ABC
from typing import Protocol
from asyncio import run
from typing import Any, Awaitable, Union
from inspect import isawaitable

ResponseT = Union[Awaitable[str], Any]

class ProtocolClass(ABC):
    def common_func(self):
        ...


class CommandsClass(ProtocolClass):
    def set(self) -> ResponseT:
        return self.common_func()
        


class SyncClass(CommandsClass):
    def common_func(self):
        print("sync")

class AsyncClass(CommandsClass):
    async def common_func(self):
        print("ASYNC")


def main():
    obj = SyncClass()
    obj.set()

async def amain():
    aobj = AsyncClass()
    a = await aobj.set()
    print(a)

main()

run(amain())

