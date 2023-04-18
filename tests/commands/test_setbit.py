from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_setbit() -> None:
    """
    Since "SETBIT" returns the old value, the test will check if previous calls set them correctly.
    """

    async with redis:
        assert await redis.setbit("setbit", offset=4, value=1) == 0

        assert await redis.setbit("setbit", offset=4, value=1) == 1
