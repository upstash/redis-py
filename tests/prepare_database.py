"""
Flush and fill the testing database with the necessary data.
"""

from asyncio import get_event_loop
from aiohttp import ClientSession
from json import dumps
from os import environ

url: str = environ["UPSTASH_REDIS_REST_URL"] + "/pipeline"
token: str = environ["UPSTASH_REDIS_REST_TOKEN"]

headers: dict[str, str] = {"Authorization": f'Bearer {token}'}

commands: list[list] = [
    # Flush the database.
    ["FLUSHDB"],

    # String.
    ["SET", "string", "test"],

    # String that will be used to test BITFIELD* commands.
    ["SET", "string_for_bitfield", "test"],

    # String that will expire on 1 January 2024. The expiry is set as a Unix timestamp.
    ["SET", "string_with_unix_expiry", "test", "EXAT", 1704067200],

    # String that's set with a numeric value.
    ["SET", "string_with_numeric_value", 1],

    # String that's set with a JSON-compatible value.
    ["SET", "string_with_json_value", dumps({"1": 2})],


    # Hash.
    ["HSET", "hash", "field_1", "test_1", "field_2", "test_2"],

    # Hash that has at least a field with a numeric value.
    ["HSET", "hash_with_numeric_value", "field_1", 1, "field_2", "2"],

    # Hash that has at least a field with a JSON-compatible value.
    ["HSET", "hash_with_json_value", "field_1", dumps({"1": 2}), "field_2", "2"],


    # List.
    ["LPUSH", "list", "element_1", "element_2"],

    # List that has at least an element with a numeric value.
    ["LPUSH", "list_with_numeric_value", 1, "2"],

    # List that has at least an element with a JSON-compatible value.
    ["LPUSH", "list_with_json_value", dumps({"1": 2}), "2"],


    # Set.
    ["SADD", "set", "test_set_value_1", "test_set_value_2"],

    # Set that has at least an element with a numeric value.
    ["SADD", "set_with_numeric_value", 1, "2"],

    # Set that has at least an element with a JSON-compatible value.
    ["SADD", "set_with_json_value", dumps({"1": 2}), "2"],


    # Sorted set.
    ["ZADD", "sorted_set", 1, "member_1", 2, "member_2"],


    # Geospatial index.
    ["GEOADD", "test_geo_index", 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania"],


    # HyperLogLog.
    ["PFADD", "test_hyperloglog", "element_1", "element_2"],

    # HyperLogLog that has at least an element with a numeric value.
    ["PFADD", "test_hyperloglog_with_numeric_value", 1, "2"],

    # HyperLogLog that has at least an element with a JSON-compatible value.
    ["PFADD", "test_hyperloglog_with_json_value", dumps({"1": 2}), "2"],
]


async def main():
    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=commands) as response:
            if response.status != 200:
                raise Exception("Failed to prepare the testing database.")

            print("success")
            await session.close()

get_event_loop().run_until_complete(main())
