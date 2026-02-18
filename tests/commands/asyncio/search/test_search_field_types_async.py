"""Async tests for KEYWORD and FACET field types."""

import json
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


@pytest.mark.asyncio
class TestAsyncKeywordField:
    """Async tests for KEYWORD field type."""

    @pytest_asyncio.fixture
    async def keyword_index(self, async_redis_client):
        name = f"test-async-keyword-{random_id()}"
        prefix = f"{name}:"
        keys = []

        redis = async_redis_client

        schema = {
            "name": "TEXT",
            "status": "KEYWORD",
            "priority": {"type": "U64", "fast": True},
        }

        index = await redis.search.create_index(
            name=name, schema=schema, data_type="string", prefixes=prefix
        )

        test_data = [
            {"name": "Task A", "status": "open", "priority": 1},
            {"name": "Task B", "status": "closed", "priority": 2},
            {"name": "Task C", "status": "open", "priority": 3},
            {"name": "Task D", "status": "in_progress", "priority": 4},
            {"name": "Task E", "status": "closed", "priority": 5},
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            await redis.set(key, json.dumps(datum))

        await index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        try:
            await index.drop()
        except Exception:
            pass
        if keys:
            await redis.delete(*keys)

    async def test_keyword_eq(self, keyword_index):
        index = keyword_index["index"]
        result = await index.query(filter={"status": {"$eq": "open"}})
        assert len(result) == 2

    async def test_keyword_in(self, keyword_index):
        index = keyword_index["index"]
        result = await index.query(filter={"status": {"$in": ["open", "closed"]}})
        assert len(result) == 4

    async def test_keyword_gt(self, keyword_index):
        index = keyword_index["index"]
        result = await index.query(filter={"status": {"$gt": "in_progress"}})
        assert len(result) >= 1

    async def test_keyword_gte(self, keyword_index):
        index = keyword_index["index"]
        result = await index.query(filter={"status": {"$gte": "open"}})
        assert len(result) >= 1

    async def test_keyword_lt(self, keyword_index):
        index = keyword_index["index"]
        result = await index.query(filter={"status": {"$lt": "open"}})
        assert len(result) >= 1

    async def test_keyword_lte(self, keyword_index):
        index = keyword_index["index"]
        result = await index.query(filter={"status": {"$lte": "open"}})
        assert len(result) >= 1


@pytest.mark.asyncio
class TestAsyncFacetField:
    """Async tests for FACET field type."""

    @pytest_asyncio.fixture
    async def facet_index(self, async_redis_client):
        name = f"test-async-facet-{random_id()}"
        prefix = f"{name}:"
        keys = []

        redis = async_redis_client

        schema = {
            "name": "TEXT",
            "category": "FACET",
            "price": {"type": "F64", "fast": True},
        }

        index = await redis.search.create_index(
            name=name, schema=schema, data_type="string", prefixes=prefix
        )

        test_data = [
            {"name": "Laptop", "category": "/electronics/computers", "price": 999.99},
            {"name": "Mouse", "category": "/electronics/peripherals", "price": 29.99},
            {"name": "Book", "category": "/books/fiction", "price": 14.99},
            {"name": "Shirt", "category": "/clothing/tops", "price": 39.99},
            {
                "name": "Keyboard",
                "category": "/electronics/peripherals",
                "price": 79.99,
            },
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            await redis.set(key, json.dumps(datum))

        await index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        try:
            await index.drop()
        except Exception:
            pass
        if keys:
            await redis.delete(*keys)

    async def test_facet_eq(self, facet_index):
        index = facet_index["index"]
        result = await index.query(
            filter={"category": {"$eq": "/electronics/peripherals"}}
        )
        assert len(result) == 2

    async def test_facet_in(self, facet_index):
        index = facet_index["index"]
        result = await index.query(
            filter={"category": {"$in": ["/electronics/peripherals", "/books/fiction"]}}
        )
        assert len(result) == 3
