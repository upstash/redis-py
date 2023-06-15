from pytest import mark
from tests.client import redis

@mark.asyncio
async def test_evalsha():
    async with redis:
        script = """
            local key1_val = tonumber(KEYS[1])
            local key2_val = tonumber(KEYS[2])
            local arg_val = tonumber(ARGV[1])
            return key1_val + key2_val + arg_val
        """
        sha1_digest = await redis.script.load(script)

        assert await redis.evalsha(sha1=sha1_digest, keys=[10, 20], args=["5"]) == 35
