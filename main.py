from upstash_redis.asyncio import Redis as ARedis
from upstash_redis import Redis
from asyncio import run

aredis = ARedis.from_env()
redis = Redis.from_env()

async def main():
    async with aredis:
        key = "asd"
        val = "dsa"
        a = await aredis.set(key, val)
        print("set:", a)

run(main())

with Redis.from_env() as r:
    r.set("ahse", "oijeroigjeoirjgoiejrgoijeroigjeoirjgo")

print("sync:", redis.get("ahse"))


redis.set("string", "asdasd")
print(redis.copy(source="string", destination="copy_destination"))


redis.set("test", "sync123")
print(redis.get("test"))


