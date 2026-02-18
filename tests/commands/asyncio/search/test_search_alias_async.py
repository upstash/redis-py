"""Async tests for search alias functionality."""

import random
import string

import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


def random_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest_asyncio.fixture
async def async_redis_client():
    return Redis.from_env()


@pytest_asyncio.fixture
async def cleanup_indexes():
    indexes = []
    yield indexes
    redis = Redis.from_env()
    for index_name in indexes:
        try:
            index = redis.search.index(index_name)
            await index.drop()
        except Exception:
            pass


@pytest.mark.asyncio
class TestAsyncAlias:
    """Async tests for alias commands."""

    async def test_add_alias_via_index(
        self, async_redis_client: Redis, cleanup_indexes
    ):
        name = f"test-async-alias-idx-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.append(name)

        index = await async_redis_client.search.create_index(
            name=name, schema={"name": "TEXT"}, data_type="string", prefixes=f"{name}:"
        )

        result = await index.add_alias(alias=alias_name)
        assert result == 1

        await async_redis_client.search.alias.delete(alias=alias_name)

    async def test_alias_add(self, async_redis_client: Redis, cleanup_indexes):
        name = f"test-async-alias-add-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.append(name)

        await async_redis_client.search.create_index(
            name=name, schema={"name": "TEXT"}, data_type="string", prefixes=f"{name}:"
        )

        result = await async_redis_client.search.alias.add(
            index_name=name, alias=alias_name
        )
        assert result == 1

        await async_redis_client.search.alias.delete(alias=alias_name)

    async def test_alias_delete(self, async_redis_client: Redis, cleanup_indexes):
        name = f"test-async-alias-del-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.append(name)

        await async_redis_client.search.create_index(
            name=name, schema={"name": "TEXT"}, data_type="string", prefixes=f"{name}:"
        )

        await async_redis_client.search.alias.add(index_name=name, alias=alias_name)

        result = await async_redis_client.search.alias.delete(alias=alias_name)
        assert result == 1

    async def test_alias_list(self, async_redis_client: Redis, cleanup_indexes):
        name = f"test-async-alias-list-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.append(name)

        await async_redis_client.search.create_index(
            name=name, schema={"name": "TEXT"}, data_type="string", prefixes=f"{name}:"
        )

        await async_redis_client.search.alias.add(index_name=name, alias=alias_name)

        aliases = await async_redis_client.search.alias.list()
        assert isinstance(aliases, dict)
        assert alias_name in aliases
        assert aliases[alias_name] == name

        await async_redis_client.search.alias.delete(alias=alias_name)

    async def test_update_alias_to_new_index(
        self, async_redis_client: Redis, cleanup_indexes
    ):
        name1 = f"test-async-alias-upd1-{random_id()}"
        name2 = f"test-async-alias-upd2-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.extend([name1, name2])

        await async_redis_client.search.create_index(
            name=name1,
            schema={"name": "TEXT"},
            data_type="string",
            prefixes=f"{name1}:",
        )
        await async_redis_client.search.create_index(
            name=name2,
            schema={"name": "TEXT"},
            data_type="string",
            prefixes=f"{name2}:",
        )

        result1 = await async_redis_client.search.alias.add(
            index_name=name1, alias=alias_name
        )
        assert result1 == 1

        result2 = await async_redis_client.search.alias.add(
            index_name=name2, alias=alias_name
        )
        assert result2 == 2

        aliases = await async_redis_client.search.alias.list()
        assert aliases[alias_name] == name2

        await async_redis_client.search.alias.delete(alias=alias_name)
