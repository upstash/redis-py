from pytest import mark

from tests.async_client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.echo(message="Upstash is nice!") == "Upstash is nice!"
