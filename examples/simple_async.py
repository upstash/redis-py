import asyncio

from upstash_redis.asyncio import Redis


async def main():
    # Reads from the environment variables
    # UPSTASH_REDIS_REST_URL
    # UPSTASH_REDIS_REST_TOKEN
    redis = Redis.from_env()

    # Set or get a key
    await redis.set("key", "value")

    # 10 is converted to "10", it's still a string
    await redis.set("key", 10)

    # The dictionary is converted to json, it's still a string
    await redis.set(
        "key",
        {
            "name": "John",
            "age": 30,
        },
    )

    # Expires in 10 seconds
    await redis.set("expire_key", value="expire_value", ex=10)

    # Gets the time to live in seconds
    await redis.ttl("expire_key")

    # Change ttl
    await redis.expire("expire_key", 20)

    # Remove ttl
    await redis.persist("expire_key")

    # Set a key only if it does not exist
    await redis.set("key", "value", nx=True)

    # Set a key only if it exists
    await redis.set("key", "value", xx=True)


asyncio.run(main())
