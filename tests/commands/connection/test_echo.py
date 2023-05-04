from pytest import mark
from tests.client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.echo(message="Upstash is nice!") == "Upstash is nice!"
