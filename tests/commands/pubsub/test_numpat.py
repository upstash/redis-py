from pytest import mark
from tests.client import redis


# TODO simulate at least one connected client.
@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.pubsub.numpat() == 0
