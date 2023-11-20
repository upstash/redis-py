from upstash_redis import Redis

# Reads from the environment variables
# UPSTASH_REDIS_REST_URL
# UPSTASH_REDIS_REST_TOKEN
redis = Redis.from_env()

# Set or get a key
redis.set("key", "value")

# 10 is converted to "10", it's still a string
redis.set("key", 10)

# The dictionary is converted to json, it's still a string
redis.set("key", {
    "name": "John",
    "age": 30,
})

# Expires in 10 seconds
redis.set("expire_key", value="expire_value", ex=10)

# Gets the time to live in seconds
assert redis.ttl("expire_key") == 10

# Change ttl
redis.expire("expire_key", 20)

# Remove ttl
redis.persist("expire_key")

# Set a key only if it does not exist
redis.set("key", "value", nx=True)

# Set a key only if it exists
redis.set("key", "value", xx=True)
