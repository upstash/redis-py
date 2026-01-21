"""Tests for search index functionality."""

import json
import time
from typing import Dict, Any, List, TypedDict, Generator

import pytest

from upstash_redis import Redis
from upstash_redis.search_index import SearchIndex


class StringIndexFixture(TypedDict):
    """Type definition for string_index fixture."""
    index: SearchIndex
    redis: Redis
    keys: List[str]
    name: str


class CountIndexFixture(TypedDict):
    """Type definition for count_index fixture."""
    index: SearchIndex
    redis: Redis
    keys: List[str]
    name: str


def random_id() -> str:
    """Generate a random ID for test isolation."""
    import random
    import string

    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest.fixture
def redis_client():
    """Create a Redis client for testing."""
    return Redis.from_env()


@pytest.fixture
def cleanup_indexes():
    """Track and cleanup indexes created during tests."""
    indexes = []

    yield indexes

    # Cleanup after test
    redis = Redis.from_env()
    for index_name in indexes:
        try:
            index = redis.search.index(index_name)
            index.drop()
        except:
            pass


class TestCreateIndex:
    """Tests for creating search indexes."""

    def test_create_string_index_simple_schema(self, redis_client, cleanup_indexes):
        """Test creating a string index with simple schema."""
        name = f"test-string-{random_id()}"
        cleanup_indexes.append(name)

        schema = {"name": "TEXT", "age": {"type": "U64", "fast": True}}

        index = redis_client.search.createIndex(
            name=name, schema=schema, dataType="string", prefix=f"{name}:"
        )

        assert index is not None
        assert index.name == name

    def test_create_string_index_nested_schema(self, redis_client, cleanup_indexes):
        """Test creating a string index with nested schema."""
        name = f"test-string-nested-{random_id()}"
        cleanup_indexes.append(name)

        schema = {
            "title": "TEXT",
            "metadata": {"author": "TEXT", "views": {"type": "U64", "fast": True}},
        }

        index = redis_client.search.createIndex(
            name=name, schema=schema, dataType="string", prefix=f"{name}:"
        )

        assert index is not None

    def test_create_hash_index(self, redis_client, cleanup_indexes):
        """Test creating a hash index."""
        name = f"test-hash-{random_id()}"
        cleanup_indexes.append(name)

        schema = {"title": "TEXT", "count": {"type": "U64", "fast": True}}

        index = redis_client.search.createIndex(
            name=name, schema=schema, dataType="hash", prefix=f"{name}:"
        )

        assert index is not None

    def test_create_json_index_simple_schema(self, redis_client, cleanup_indexes):
        """Test creating a JSON index with simple schema."""
        name = f"test-json-{random_id()}"
        cleanup_indexes.append(name)

        schema = {"name": "TEXT", "age": {"type": "U64", "fast": True}}

        index = redis_client.search.createIndex(
            name=name, schema=schema, dataType="json", prefix=f"{name}:"
        )

        assert index is not None
        assert index.name == name

    def test_create_json_index_nested_schema(self, redis_client, cleanup_indexes):
        """Test creating a JSON index with nested schema."""
        name = f"test-json-nested-{random_id()}"
        cleanup_indexes.append(name)

        schema = {
            "title": "TEXT",
            "metadata": {"author": "TEXT", "views": {"type": "U64", "fast": True}},
        }

        index = redis_client.search.createIndex(
            name=name, schema=schema, dataType="json", prefix=f"{name}:"
        )

        assert index is not None

    def test_create_index_with_language(self, redis_client, cleanup_indexes):
        """Test creating an index with language option."""
        name = f"test-lang-{random_id()}"
        cleanup_indexes.append(name)

        schema = {"content": "TEXT"}

        index = redis_client.search.createIndex(
            name=name,
            schema=schema,
            dataType="string",
            prefix=f"{name}:",
            language="turkish",
        )

        assert index is not None

    def test_create_index_with_multiple_prefixes(self, redis_client, cleanup_indexes):
        """Test creating an index with multiple prefixes."""
        name = f"test-multi-prefix-{random_id()}"
        cleanup_indexes.append(name)

        schema = {"name": "TEXT"}

        index = redis_client.search.createIndex(
            name=name,
            schema=schema,
            dataType="hash",
            prefix=[f"{name}:users:", f"{name}:profiles:"],
        )

        assert index is not None


class TestQueryString:
    """Tests for querying string indexes."""

    @pytest.fixture(scope="class")
    def string_index(self) -> Generator[StringIndexFixture, None, None]:
        """Create a string index with test data."""
        name = f"test-query-string-{random_id()}"
        prefix = f"{name}:"
        keys = []

        redis = Redis.from_env()

        schema = {
            "name": "TEXT",
            "description": "TEXT",
            "category": {"type": "TEXT", "noTokenize": True},
            "price": {"type": "F64", "fast": True},
            "stock": {"type": "U64", "fast": True},
            "active": "BOOL",
        }

        index = redis.search.createIndex(
            name=name, schema=schema, dataType="string", prefix=prefix
        )

        # Add test data
        test_data = [
            {
                "name": "Laptop Pro",
                "description": "High performance laptop",
                "category": "electronics",
                "price": 1299.99,
                "stock": 50,
                "active": True,
            },
            {
                "name": "Laptop Basic",
                "description": "Budget friendly laptop",
                "category": "electronics",
                "price": 599.99,
                "stock": 100,
                "active": True,
            },
            {
                "name": "Wireless Mouse",
                "description": "Ergonomic wireless mouse",
                "category": "electronics",
                "price": 29.99,
                "stock": 200,
                "active": True,
            },
            {
                "name": "USB Cable",
                "description": "Fast charging USB cable",
                "category": "accessories",
                "price": 9.99,
                "stock": 500,
                "active": True,
            },
            {
                "name": "Phone Case",
                "description": "Protective phone case",
                "category": "accessories",
                "price": 19.99,
                "stock": 300,
                "active": False,
            },
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            redis.set(key, json.dumps(datum))

        # Wait for indexing
        index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        # Cleanup
        try:
            index.drop()
        except:
            pass
        if keys:
            redis.delete(*keys)

    def test_query_text_eq(self, string_index: StringIndexFixture):
        """Test querying with $eq on text field."""
        index = string_index["index"]
        result = index.query(filter={"name": {"$eq": "Laptop"}})

        assert len(result) > 0

    def test_query_text_fuzzy(self, string_index: StringIndexFixture):
        """Test querying with fuzzy search for typo tolerance."""
        index = string_index["index"]
        result = index.query(filter={"name": {"$fuzzy": "laptopp"}})

        assert len(result) > 0

    def test_query_text_phrase(self, string_index: StringIndexFixture):
        """Test querying with phrase matching."""
        index = string_index["index"]
        result = index.query(filter={"description": {"$phrase": "wireless mouse"}})

        assert len(result) > 0

    def test_query_text_regex(self, string_index: StringIndexFixture):
        """Test querying with regex pattern."""
        index = string_index["index"]
        result = index.query(filter={"name": {"$regex": "Laptop.*"}})

        assert len(result) > 0

    def test_query_numeric_gt(self, string_index: StringIndexFixture):
        """Test querying with $gt on numeric field."""
        index = string_index["index"]
        result = index.query(filter={"price": {"$gt": 500}})

        assert len(result) > 0

    def test_query_numeric_gte(self, string_index: StringIndexFixture):
        """Test querying with $gte on numeric field."""
        index = string_index["index"]
        result = index.query(filter={"stock": {"$gte": 100}})

        assert len(result) > 0

    def test_query_numeric_lt(self, string_index: StringIndexFixture):
        """Test querying with $lt on numeric field."""
        index = string_index["index"]
        result = index.query(filter={"price": {"$lt": 50}})

        assert len(result) > 0

    def test_query_numeric_lte(self, string_index: StringIndexFixture):
        """Test querying with $lte on numeric field."""
        index = string_index["index"]
        result = index.query(filter={"stock": {"$lte": 50}})

        assert len(result) > 0

    def test_query_boolean_eq(self, string_index: StringIndexFixture):
        """Test querying with $eq on boolean field."""
        index = string_index["index"]
        result = index.query(filter={"active": {"$eq": False}})

        assert len(result) > 0

    def test_query_with_limit(self, string_index: StringIndexFixture):
        """Test querying with limit option."""
        index = string_index["index"]
        result = index.query(filter={"category": {"$eq": "electronics"}}, limit=2)

        assert len(result) > 0
        assert len(result) <= 2

    def test_query_with_pagination(self, string_index: StringIndexFixture):
        """Test querying with offset for pagination."""
        index = string_index["index"]
        first_page = index.query(
            filter={"category": {"$eq": "electronics"}}, limit=2, offset=0
        )

        second_page = index.query(
            filter={"category": {"$eq": "electronics"}}, limit=2, offset=2
        )

        assert len(first_page) > 0
        # second_page might be empty if there are only 2-3 results

    def test_query_with_sort_asc(self, string_index: StringIndexFixture):
        """Test querying with sortBy ascending."""
        index = string_index["index"]
        result = index.query(
            filter={"category": {"$eq": "electronics"}},
            orderBy={"price": "ASC"},
            limit=3,
        )

        assert len(result) > 0

    def test_query_with_sort_desc(self, string_index: StringIndexFixture):
        """Test querying with sortBy descending."""
        index = string_index["index"]
        result = index.query(
            filter={"category": {"$eq": "electronics"}},
            orderBy={"price": "DESC"},
            limit=3,
        )

        assert len(result) > 0

    def test_query_no_content(self, string_index: StringIndexFixture):
        """Test querying with noContent (keys only)."""
        index = string_index["index"]
        result = index.query(filter={"category": {"$eq": "electronics"}}, select={})

        assert len(result) > 0

    def test_query_with_return_fields(self, string_index: StringIndexFixture):
        """Test querying with specific return fields."""
        index = string_index["index"]
        result = index.query(
            filter={"category": {"$eq": "electronics"}},
            select={"category": True},
            highlight={"fields": []},
        )

        assert len(result) > 0


class TestCount:
    """Tests for count functionality."""

    @pytest.fixture(scope="class")
    def count_index(self) -> Generator[CountIndexFixture, None, None]:
        """Create an index for count testing."""
        name = f"test-count-{random_id()}"
        prefix = f"{name}:"
        keys = []

        redis = Redis.from_env()

        schema = {"type": "TEXT", "value": {"type": "U64", "fast": True}}

        index = redis.search.createIndex(
            name=name, schema=schema, dataType="string", prefix=prefix
        )

        # Add test data
        for i in range(10):
            key = f"{prefix}{i}"
            keys.append(key)
            redis.set(key, json.dumps({"type": "A" if i < 5 else "B", "value": i}))

        index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        # Cleanup
        try:
            index.drop()
        except:
            pass
        if keys:
            redis.delete(*keys)

    def test_count_matching_documents(self, count_index: CountIndexFixture):
        """Test counting matching documents."""
        index = count_index["index"]
        result = index.count({"type": {"$eq": "A"}})

        assert "count" in result
        assert result["count"] > 0

    def test_count_with_numeric_filter(self, count_index: CountIndexFixture):
        """Test counting with numeric filter."""
        index = count_index["index"]
        result = index.count({"value": {"$eq": 5}})

        assert "count" in result
        assert result["count"] > 0


class TestDescribe:
    """Tests for describe functionality."""

    def test_describe_index(self, redis_client, cleanup_indexes):
        """Test describing an index structure."""
        name = f"test-describe-{random_id()}"
        cleanup_indexes.append(name)

        schema = {
            "title": {"type": "TEXT", "noStem": True},
            "count": {"type": "U64", "fast": True},
            "active": "BOOL",
        }

        index = redis_client.search.createIndex(
            name=name, schema=schema, dataType="string", prefix=f"{name}:"
        )

        description = index.describe()

        assert description is not None
        assert "name" in description or "schema" in description


class TestDrop:
    """Tests for drop functionality."""

    def test_drop_existing_index(self, redis_client):
        """Test dropping an existing index."""
        name = f"test-drop-{random_id()}"

        schema = {"name": "TEXT"}

        index = redis_client.search.createIndex(
            name=name, schema=schema, dataType="string", prefix=f"{name}:"
        )

        result = index.drop()
        # Result should be 1 or 0 or "OK"
        assert result is not None


class TestIndexMethod:
    """Tests for redis.search.index method."""

    def test_index_without_schema(self, redis_client):
        """Test creating a SearchIndex instance without schema."""
        index = redis_client.search.index("test-index")

        assert index is not None
        assert index.name == "test-index"
        assert index.schema is None

    def test_index_with_schema(self, redis_client):
        """Test creating a SearchIndex instance with schema."""
        schema = {"name": "TEXT", "age": {"type": "U64", "fast": True}}

        index = redis_client.search.index("test-index", schema)

        assert index is not None
        assert index.name == "test-index"
        assert index.schema == schema
