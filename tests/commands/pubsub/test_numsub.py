from pytest import mark
from tests.client import redis


# TODO simulate at least one connected client.
@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.pubsub.numsub("test") == {"test": 0}


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.pubsub.numsub("test") == ["test", 0]

    redis.format_return = True
