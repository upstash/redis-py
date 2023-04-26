"""
Flush and fill the testing database with the necessary data.
"""

from asyncio import get_event_loop
from aiohttp import ClientSession
from os import environ

url: str = environ["UPSTASH_REDIS_REST_URL"] + "/pipeline"
token: str = environ["UPSTASH_REDIS_REST_TOKEN"]

headers: dict[str, str] = {"Authorization": f'Bearer {token}'}

commands: list[list] = [
    # Flush the database.
    ["FLUSHDB"],

    # String.
    ["SET", "string", "test"],

    # String that will expire on 1 January 2024. The expiry is set as a Unix timestamp.
    ["SET", "string_with_unix_expiry", "test", "EXAT", 1704067200],

    # Strings to be used when testing BITFIELD* commands.
    ["SET", "string_for_bitfield_set", "test"],
    ["SET", "string_for_bitfield_incrby", "test"],
    ["SET", "string_for_bitfield_chained_commands", "test"],
    ["SET", "string_for_bitfield_sat_overflow", "test"],
    ["SET", "string_for_bitfield_wrap_overflow", "test"],
    ["SET", "string_for_bitfield_fail_overflow", "test"],

    # Strings to be used as source keys when testing BITOP.
    ["SET", "string_as_bitop_source_1", "abcd"],
    ["SET", "string_as_bitop_source_2", "1234"],

    # String that will be used as a destination key to test COPY with REPLACE.
    ["SET", "string_as_copy_destination", "test"],

    # String to be deleted when testing DELETE.
    ["SET", "string_for_delete_1", "a"],
    ["SET", "string_for_delete_2", "a"],

    # String to be used when testing expiry-setting commands.
    ["SET", "string_for_expire", "a"],


    # Hash.
    ["HSET", "hash", "field_1", "test_1", "field_2", "test_2"],


    # List.
    ["LPUSH", "list", "element_1", "element_2"],


    # Set.
    ["SADD", "set", "test_set_value_1", "test_set_value_2"],


    # Sorted set.
    ["ZADD", "sorted_set", 1, "member_1", 2, "member_2"],


    # Geospatial index.
    ["GEOADD", "test_geo_index", 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania"],


    # HyperLogLog.
    ["PFADD", "test_hyperloglog", "element_1", "element_2"],
]


async def main():
    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=commands) as response:
            if response.status != 200:
                raise Exception("Failed to prepare the testing database.")

            print("success")
            await session.close()

get_event_loop().run_until_complete(main())
