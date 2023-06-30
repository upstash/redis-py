from upstash_redis.asyncio import Redis as ARedis
from upstash_redis import Redis
from asyncio import run

aredis = ARedis.from_env()
redis = Redis.from_env()

# async def main():
#     async with redis:
#         key = "asd"
#         val = "dsa"
#         a = await aredis.set(key, val)
#         print("set:", a)

# run(main())

redis.set("test", "sync123")
print(redis.get("test"))

# redis.copy(source="string", destination="copy_destination")







