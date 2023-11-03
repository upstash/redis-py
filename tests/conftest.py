from os import environ
from typing import Dict, List

import pytest
import pytest_asyncio
import requests

from upstash_redis import Redis
from upstash_redis.asyncio import Redis as AsyncRedis

"""
Flush and fill the testing database with the necessary data.
"""

url: str = environ["UPSTASH_REDIS_REST_URL"] + "/pipeline"
token: str = environ["UPSTASH_REDIS_REST_TOKEN"]

headers: Dict[str, str] = {"Authorization": f"Bearer {token}"}

commands: List[List] = [
    # Flush the database.
    ["FLUSHDB"],
    [
        "MSET",
        # String.
        "string",
        "test",
        # Strings to be used when testing BITFIELD* commands.
        "string_for_bitfield_set",
        "test",
        "string_for_bitfield_incrby",
        "test",
        "string_for_bitfield_chained_commands",
        "test",
        "string_for_bitfield_overflow",
        "test",
        # Strings to be used as source keys when testing BITOP.
        "string_as_bitop_source_1",
        "abcd",
        "string_as_bitop_source_2",
        "1234",
        # String that will be used as a destination key to test COPY with REPLACE.
        "string_as_copy_destination_with_replace",
        "a",
        # Strings to be deleted when testing DELETE.
        "string_for_delete_1",
        "a",
        "string_for_delete_2",
        "a",
        # Strings to be renamed when testing RENAME*.
        "string_for_rename",
        "test",
        "string_for_renamenx",
        "test",
        # Strings to be used when testing expiry-related commands.
        "string_for_expire",
        "a",
        "string_for_expireat",
        "a",
        "string_for_pexpire",
        "a",
        "string_for_pexpireat",
        "a",
        # Strings for testing datetime with expire
        "string_for_expire_dt",
        "a",
        "string_for_expireat_dt",
        "a",
        "string_for_pexpire_dt",
        "a",
        "string_for_pexpireat_dt",
        "a",
        #Other expire stuff
        "string_for_persist",
        "a",
        "string_for_ttl",
        "a",
        "string_for_pttl",
        "a",
        # Strings to be asynchronously deleted when testing UNLINK.
        "string_for_unlink_1",
        "a",
        "string_for_unlink_2",
        "a",
    ],
    # Hash.
    ["HSET", "hash", "field_1", "test_1", "field_2", "test_2"],
    # List.
    ["LPUSH", "list", "element_1", "element_2"],
    # Set.
    ["SADD", "set", "test_set_value_1", "test_set_value_2"],
    # Sorted set.
    ["ZADD", "sorted_set", 1, "member_1", 2, "member_2"],
    # Geospatial index.
    [
        "GEOADD",
        "test_geo_index",
        13.361389,
        38.115556,
        "Palermo",
        15.087269,
        37.502669,
        "Catania",
        43.486391,
        -35.283347,
        "random",
    ],
    # HyperLogLog.
    ["PFADD", "hyperloglog", "element_1", "element_2"],
    # HyperLogLogs to be used when testing
    ["PFADD", "hyperloglog_for_pfmerge_1", "1", "2", "3", "4"],
    ["PFADD", "hyperloglog_for_pfmerge_2", "1", "2", "3"],
]


def pytest_configure():
    with requests.post(url, headers=headers, json=commands) as r:
        if r.status_code != 200:
            raise RuntimeError(r.json()["error"])


@pytest_asyncio.fixture
async def async_redis():
    async with AsyncRedis.from_env(allow_telemetry=False) as redis:
        yield redis


@pytest.fixture
def redis():
    with Redis.from_env(allow_telemetry=False) as redis:
        yield redis
