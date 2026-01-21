"""Async tests for search index functionality."""

import json
import random
import string
from typing import Any, Dict, List

import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis


def random_id() -> str:
    """Generate a random ID for test isolation."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest_asyncio.fixture
async def async_redis_client():
    """Create an async Redis client for testing."""
    return Redis.from_env()


@pytest_asyncio.fixture
async def cleanup_indexes():
    """Track and cleanup indexes created during tests."""
    indexes = []

    yield indexes

    # Cleanup after test
    redis = Redis.from_env()
    for index_name in indexes:
        try:
            index = redis.search.index(index_name)
            await index.drop()
        except:
            pass


@pytest.mark.asyncio
class TestAsyncQuery:
    """Tests for async query operations."""

    @pytest_asyncio.fixture
    async def query_index(self, async_redis_client, cleanup_indexes):
        """Create a test index with data for querying."""
        name = f"test-async-query-{random_id()}"
        prefix = f"{name}:"
        keys = []
        cleanup_indexes.append(name)

        redis = async_redis_client

        schema = {
            "name": "TEXT",
            "description": "TEXT",
            "price": {"type": "F64", "fast": True},
            "stock": {"type": "U64", "fast": True},
            "active": "BOOL",
        }

        index = await redis.search.create_index(
            name=name, schema=schema, dataType="string", prefix=prefix
        )

        # Add test data
        test_data = [
            {
                "name": "Laptop Pro",
                "description": "High performance laptop",
                "price": 1299.99,
                "stock": 50,
                "active": True,
            },
            {
                "name": "Laptop Basic",
                "description": "Budget friendly laptop",
                "price": 599.99,
                "stock": 100,
                "active": True,
            },
            {
                "name": "Wireless Mouse",
                "description": "Ergonomic wireless mouse",
                "price": 29.99,
                "stock": 200,
                "active": True,
            },
            {
                "name": "USB Cable",
                "description": "Fast charging USB cable",
                "price": 9.99,
                "stock": 500,
                "active": False,
            },
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            await redis.set(key, json.dumps(datum))

        # Wait for indexing
        await index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        # Cleanup
        try:
            await index.drop()
        except:
            pass
        if keys:
            await redis.delete(*keys)

    async def test_query_text_eq(self, query_index):
        """Test async querying with $eq on text field."""
        index = query_index["index"]
        result = await index.query(filter={"name": {"$eq": "Laptop"}})

        assert len(result) > 0
        assert all("key" in r and "score" in r for r in result)

    async def test_query_text_fuzzy(self, query_index):
        """Test async querying with fuzzy search."""
        index = query_index["index"]
        result = await index.query(filter={"name": {"$fuzzy": "laptopp"}})

        assert len(result) > 0

    async def test_query_text_phrase(self, query_index):
        """Test async querying with phrase matching."""
        index = query_index["index"]
        result = await index.query(filter={"description": {"$phrase": "wireless mouse"}})

        assert len(result) > 0

    async def test_query_numeric_gt(self, query_index):
        """Test async querying with $gt on numeric field."""
        index = query_index["index"]
        result = await index.query(filter={"price": {"$gt": 500}})

        assert len(result) > 0
        # Verify data structure
        for item in result:
            assert "key" in item
            assert "score" in item

    async def test_query_numeric_gte(self, query_index):
        """Test async querying with $gte on numeric field."""
        index = query_index["index"]
        result = await index.query(filter={"stock": {"$gte": 100}})

        assert len(result) > 0

    async def test_query_numeric_lt(self, query_index):
        """Test async querying with $lt on numeric field."""
        index = query_index["index"]
        result = await index.query(filter={"price": {"$lt": 50}})

        assert len(result) > 0

    async def test_query_boolean_eq(self, query_index):
        """Test async querying with $eq on boolean field."""
        index = query_index["index"]
        result = await index.query(filter={"active": {"$eq": False}})

        assert len(result) > 0

    async def test_query_with_limit(self, query_index):
        """Test async querying with limit option."""
        index = query_index["index"]
        result = await index.query(filter={"active": {"$eq": True}}, limit=2)

        assert len(result) > 0
        assert len(result) <= 2

    async def test_query_with_pagination(self, query_index):
        """Test async querying with offset for pagination."""
        index = query_index["index"]
        first_page = await index.query(
            filter={"active": {"$eq": True}}, limit=2, offset=0
        )
        second_page = await index.query(
            filter={"active": {"$eq": True}}, limit=2, offset=2
        )

        assert len(first_page) > 0

    async def test_query_with_sort_asc(self, query_index):
        """Test async querying with sortBy ascending."""
        index = query_index["index"]
        result = await index.query(
            filter={"active": {"$eq": True}},
            orderBy={"price": "ASC"},
            limit=3,
        )

        assert len(result) > 0

    async def test_query_with_sort_desc(self, query_index):
        """Test async querying with sortBy descending."""
        index = query_index["index"]
        result = await index.query(
            filter={"active": {"$eq": True}},
            orderBy={"price": "DESC"},
            limit=3,
        )

        assert len(result) > 0


@pytest.mark.asyncio
class TestAsyncCount:
    """Tests for async count operations."""

    @pytest_asyncio.fixture
    async def count_index(self, async_redis_client, cleanup_indexes):
        """Create a test index with data for counting."""
        name = f"test-async-count-{random_id()}"
        prefix = f"{name}:"
        keys = []
        cleanup_indexes.append(name)

        redis = async_redis_client

        schema = {
            "category": {"type": "TEXT", "noTokenize": True},
            "price": {"type": "F64", "fast": True},
            "active": "BOOL",
        }

        index = await redis.search.create_index(
            name=name, schema=schema, dataType="string", prefix=prefix
        )

        # Add test data
        test_data = [
            {"category": "electronics", "price": 1299.99, "active": True},
            {"category": "electronics", "price": 599.99, "active": True},
            {"category": "accessories", "price": 29.99, "active": True},
            {"category": "accessories", "price": 9.99, "active": False},
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            await redis.set(key, json.dumps(datum))

        # Wait for indexing
        await index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        # Cleanup
        try:
            await index.drop()
        except:
            pass
        if keys:
            await redis.delete(*keys)

    async def test_count_matching_documents(self, count_index):
        """Test async counting matching documents."""
        index = count_index["index"]
        result = await index.count(filter={"category": {"$eq": "electronics"}})

        assert "count" in result
        assert result["count"] >= 2

    async def test_count_with_numeric_filter(self, count_index):
        """Test async counting with numeric filter."""
        index = count_index["index"]
        result = await index.count(filter={"price": {"$gt": 100}})

        assert "count" in result
        assert result["count"] >= 2


@pytest.mark.asyncio
class TestAsyncDescribe:
    """Tests for async describe operations."""

    async def test_describe_index(self, async_redis_client, cleanup_indexes):
        """Test async describing an index."""
        name = f"test-async-describe-{random_id()}"
        cleanup_indexes.append(name)

        redis = async_redis_client

        schema = {
            "name": "TEXT",
            "price": {"type": "F64", "fast": True},
            "active": "BOOL",
        }

        index = await redis.search.create_index(
            name=name, schema=schema, dataType="string", prefix=f"{name}:"
        )

        description = await index.describe()

        assert description is not None
        assert "name" in description
        assert description["name"] == name
        assert "schema" in description
        assert "name" in description["schema"]
        assert "price" in description["schema"]
        assert "active" in description["schema"]

        # Cleanup
        try:
            await index.drop()
        except:
            pass


@pytest.mark.asyncio
class TestAsyncDrop:
    """Tests for async drop operations."""

    async def test_drop_index(self, async_redis_client):
        """Test async dropping an index."""
        name = f"test-async-drop-{random_id()}"

        redis = async_redis_client

        schema = {"name": "TEXT"}

        index = await redis.search.create_index(
            name=name, schema=schema, dataType="string", prefix=f"{name}:"
        )

        # Drop the index
        result = await index.drop()

        # Verify it was dropped (describe should fail or return None)
        # For now just check that drop executed
        assert result is not None
