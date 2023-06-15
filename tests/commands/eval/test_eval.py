from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_eval():
    async with redis:
        script = """
            local key1_val = tonumber(KEYS[1])
            local key2_val = tonumber(KEYS[2])
            local arg_val = tonumber(ARGV[1])
            return key1_val + key2_val + arg_val
        """
        assert await redis.eval(script=script, keys=["10", "20"], args=["5"]) == 35


@mark.asyncio
async def test_eval_with_list_return():
    async with redis:
        script = """
            return { KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3] }
        """
        keys = ["key1", "key2"]
        arguments = ["arg1", "arg2", "arg3"]

        res = keys
        res.extend(arguments)

        assert await redis.eval(script=script, keys=keys, args=arguments) == res


