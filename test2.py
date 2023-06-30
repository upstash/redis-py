import asyncio
from typing import Awaitable, TypeVar

T = TypeVar('T')

class AsyncClass:
    async def async_function(self) -> Awaitable[str]:
        await asyncio.sleep(1)
        return "Async result"


class SyncClass(AsyncClass):
    def __init__(self, async_instance: AsyncClass):
        super().__init__()
        self.async_instance = async_instance

    def __getattr__(self, name):
        async_func = getattr(self.async_instance, name)

        def sync_func(*args, **kwargs) -> T:
            return asyncio.run(async_func(*args, **kwargs))

        return sync_func


# Example usage with autocompletion
async_instance = AsyncClass()
sync_instance = SyncClass(async_instance)

result_sync: str = sync_instance.async_function()
print("Synchronous result:", result_sync)


async def async_usage():
    async_instance = AsyncClass()
    sync_instance = SyncClass(async_instance)

    result_async: str = await async_instance.async_function()
    print("Asynchronous result:", result_async)

asyncio.run(async_usage())
