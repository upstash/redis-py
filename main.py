from upstash_redis.asyncio import Redis as ARedis
from upstash_redis import Redis
from asyncio import run

aredis = ARedis.from_env()
redis = Redis.from_env()

async def main():
    async with redis:
        key = "asd"
        val = "dsa"
        a = await aredis.set(key, val)
        print("set:", a)

a = redis.bitfield("asd").get("asd", "asd").execute()




run(main())

