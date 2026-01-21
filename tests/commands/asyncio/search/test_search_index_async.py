"""Async tests for search index functionality."""

import json
import random
import string
from typing import Any, Dict, List, TypedDict

import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis
from upstash_redis.commands import AsyncSearchIndexCommands


class QueryIndexFixture(TypedDict):
    """Type definition for query_index fixture."""
    index: AsyncSearchIndexCommands
    redis: Redis
    keys: List[str]
    name: str


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
    async def query_index(self, async_redis_client: Redis, cleanup_indexes):
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

    async def test_query_text_eq(self, query_index: QueryIndexFixture):
        """Test async querying with $eq on text field."""
        index = query_index["index"]
        result = await index.query(filter={"name": {"$eq": "Laptop"}})

        assert len(result) == 2
        for r in result:
            assert "key" in r
            # Verify key has the expected prefix
            assert query_index["name"] in r["key"]
            # Verify score is numeric
            assert "score" in r
            assert isinstance(float(r["score"]), float)

    async def test_query_text_fuzzy(self, query_index: QueryIndexFixture):
        """Test async querying with fuzzy search."""
        index = query_index["index"]
        result = await index.query(filter={"name": {"$fuzzy": "laptopp"}})

        # Fuzzy search should find Laptop variants despite typo
        assert len(result) >= 2
        assert all("key" in r and "score" in r for r in result)

    async def test_query_text_phrase(self, query_index: QueryIndexFixture):
        """Test async querying with phrase matching."""
        index = query_index["index"]
        result = await index.query(filter={"description": {"$phrase": "wireless mouse"}})

        # Should find the Wireless Mouse item
        assert len(result) == 1
        assert "key" in result[0]
        assert "score" in result[0]
        assert isinstance(float(result[0]["score"]), float)
        # Check data field exists and has expected fields
        if "data" in result[0]:
            assert "name" in result[0]["data"] or "description" in result[0]["data"]

    async def test_query_numeric_gt(self, query_index: QueryIndexFixture):
        """Test async querying with $gt on numeric field."""
        index = query_index["index"]
        result = await index.query(filter={"price": {"$gt": 500}})

        # Should find Laptop Pro (1299.99) and Laptop Basic (599.99)
        assert len(result) == 2
        for item in result:
            assert "key" in item
            assert "score" in item
            assert isinstance(float(item["score"]), float)
            # Check data field exists with price field
            if "data" in item:
                assert "price" in item["data"]
                # Verify price is greater than 500
                price_value = item["data"]["price"]
                assert isinstance(price_value, (int, float, str))
                assert float(price_value) > 500

    async def test_query_numeric_gte(self, query_index: QueryIndexFixture):
        """Test async querying with $gte on numeric field."""
        index = query_index["index"]
        result = await index.query(filter={"stock": {"$gte": 100}})

        # Should find items with stock >= 100 (Laptop Basic, Mouse, Cable)
        assert len(result) == 3
        assert all("key" in r and "score" in r for r in result)

    async def test_query_numeric_lt(self, query_index: QueryIndexFixture):
        """Test async querying with $lt on numeric field."""
        index = query_index["index"]
        result = await index.query(filter={"price": {"$lt": 50}})

        # Should find Wireless Mouse (29.99) and USB Cable (9.99)
        assert len(result) == 2
        assert all("key" in r and "score" in r for r in result)

    async def test_query_boolean_eq(self, query_index: QueryIndexFixture):
        """Test async querying with $eq on boolean field."""
        index = query_index["index"]
        result = await index.query(filter={"active": {"$eq": False}})

        # Should find USB Cable (active: False)
        assert len(result) == 1
        assert "key" in result[0]
        assert "score" in result[0]
        assert isinstance(float(result[0]["score"]), float)
        # Check data field has active field
        if "data" in result[0]:
            assert "active" in result[0]["data"]
            # Verify active is False
            assert result[0]["data"]["active"] in (False, "false", "False", 0, "0")

    async def test_query_with_limit(self, query_index: QueryIndexFixture):
        """Test async querying with limit option."""
        index = query_index["index"]
        result = await index.query(filter={"active": {"$eq": True}}, limit=2)

        # Limit should restrict results to 2 even though 3 items match
        assert len(result) == 2
        assert all("key" in r and "score" in r for r in result)

    async def test_query_with_pagination(self, query_index: QueryIndexFixture):
        """Test async querying with offset for pagination."""
        index = query_index["index"]
        first_page = await index.query(
            filter={"active": {"$eq": True}}, limit=2, offset=0
        )
        second_page = await index.query(
            filter={"active": {"$eq": True}}, limit=2, offset=2
        )

        # Should get 2 items on first page
        assert len(first_page) == 2
        # Should get 1 item on second page (3 total active items)
        assert len(second_page) == 1
        # Pages should have different items
        first_keys = {r["key"] for r in first_page}
        second_keys = {r["key"] for r in second_page}
        assert first_keys.isdisjoint(second_keys)

    async def test_query_with_sort_asc(self, query_index: QueryIndexFixture):
        """Test async querying with sortBy ascending."""
        index = query_index["index"]
        result = await index.query(
            filter={"active": {"$eq": True}},
            orderBy={"price": "ASC"},
            limit=3,
        )

        # Should get 3 active items sorted by price ascending
        assert len(result) == 3
        assert all("key" in r and "score" in r for r in result)

    async def test_query_with_sort_desc(self, query_index: QueryIndexFixture):
        """Test async querying with sortBy descending."""
        index = query_index["index"]
        result = await index.query(
            filter={"active": {"$eq": True}},
            orderBy={"price": "DESC"},
            limit=3,
        )

        # Should get 3 active items sorted by price descending
        assert len(result) == 3
        assert all("key" in r and "score" in r for r in result)

    async def test_query_with_all_fields(self, query_index: QueryIndexFixture):
        """Test querying returns all fields when select is not specified."""
        index = query_index["index"]
        result = await index.query(filter={"name": {"$eq": "Wireless Mouse"}}, limit=1)

        # Should get complete result with all fields
        assert len(result) == 1
        item = result[0]
        
        # Verify structure
        assert "key" in item
        assert "score" in item
        assert isinstance(float(item["score"]), float)
        assert "data" in item
        
        # Verify all fields are present in data (values are strings for string dataType)
        data = item["data"]
        assert data["name"] == "Wireless Mouse"
        assert data["description"] == "Ergonomic wireless mouse"
        assert float(data["price"]) == 29.99
        assert int(data["stock"]) == 200
        assert data["active"] in (True, "true", "True")

    async def test_query_with_no_content(self, query_index: QueryIndexFixture):
        """Test querying with select={{}} returns only keys and scores."""
        index = query_index["index"]
        result = await index.query(
            filter={"name": {"$eq": "Wireless Mouse"}},
            select={},
            limit=1
        )

        # Should get only key and score, no data
        assert len(result) == 1
        item = result[0]
        
        assert "key" in item
        assert "score" in item
        assert isinstance(float(item["score"]), float)
        # With noContent, data field should not be present
        assert "data" not in item

    async def test_query_with_specific_field(self, query_index: QueryIndexFixture):
        """Test querying with select={{"price": True}} returns only specified field."""
        index = query_index["index"]
        result = await index.query(
            filter={"name": {"$eq": "Wireless Mouse"}},
            select={"price": True},
            limit=1
        )

        # Should get key, score, and only the price field
        assert len(result) == 1
        item = result[0]
        
        assert "key" in item
        assert "score" in item
        assert isinstance(float(item["score"]), float)
        assert "data" in item
        
        # Verify only price field is present (value is string for string dataType)
        data = item["data"]
        assert "price" in data
        assert float(data["price"]) == 29.99
        # Other fields should not be present
        assert "name" not in data
        assert "description" not in data
        assert "stock" not in data
        assert "active" not in data


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

    async def test_describe_index(self, async_redis_client: Redis, cleanup_indexes):
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
