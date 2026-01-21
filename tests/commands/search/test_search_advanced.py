"""Tests for JSON and nested schema search functionality."""

import json
from typing import List, TypedDict, Generator

import pytest

from upstash_redis import Redis
from upstash_redis.search_index import SearchIndex


class IndexFixture(TypedDict):
    """Type definition for index fixtures."""
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


class TestQueryJson:
    """Tests for querying JSON indexes."""

    @pytest.fixture(scope="class")
    def json_index(self) -> Generator[IndexFixture, None, None]:
        """Create a JSON index with test data."""
        name = f"test-query-json-{random_id()}"
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
            name=name, schema=schema, dataType="json", prefix=prefix
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
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            redis.json.set(key, "$", datum)

        index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        # Cleanup
        try:
            index.drop()
        except:
            pass
        if keys:
            redis.delete(*keys)

    def test_query_json_text_eq(self, json_index: IndexFixture):
        """Test querying JSON index with $eq on text field."""
        index = json_index["index"]
        result = index.query(filter={"name": {"$eq": "Laptop"}})

        assert len(result) > 0

    def test_query_json_fuzzy(self, json_index: IndexFixture):
        """Test querying JSON index with fuzzy search."""
        index = json_index["index"]
        result = index.query(filter={"name": {"$fuzzy": "laptopp"}})

        assert len(result) > 0

    def test_query_json_phrase(self, json_index: IndexFixture):
        """Test querying JSON index with phrase matching."""
        index = json_index["index"]
        result = index.query(filter={"description": {"$phrase": "wireless mouse"}})

        assert len(result) > 0

    def test_query_json_regex(self, json_index: IndexFixture):
        """Test querying JSON index with regex pattern."""
        index = json_index["index"]
        result = index.query(filter={"name": {"$regex": "Laptop.*"}})

        assert len(result) > 0

    def test_query_json_numeric_gt(self, json_index: IndexFixture):
        """Test querying JSON index with $gt on numeric field."""
        index = json_index["index"]
        result = index.query(filter={"price": {"$gt": 500}})

        assert len(result) > 0

    def test_query_json_with_sorting(self, json_index: IndexFixture):
        """Test querying JSON index with sorting."""
        index = json_index["index"]
        result = index.query(
            filter={"category": {"$eq": "electronics"}},
            orderBy={"price": "DESC"},
            limit=3,
        )

        assert len(result) > 0


class TestHashIndex:
    """Tests for hash index queries."""

    @pytest.fixture(scope="class")
    def hash_index(self) -> Generator[IndexFixture, None, None]:
        """Create a hash index with test data."""
        name = f"test-hash-query-{random_id()}"
        prefix = f"{name}:"
        keys = []

        redis = Redis.from_env()

        schema = {"name": "TEXT", "score": {"type": "U64", "fast": True}}

        index = redis.search.createIndex(
            name=name, schema=schema, dataType="hash", prefix=prefix
        )

        # Add test data using HSET
        test_data = [
            {"name": "Alice", "score": "95"},
            {"name": "Bob", "score": "87"},
            {"name": "Charlie", "score": "92"},
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            redis.hset(key, values=datum)

        index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        # Cleanup
        try:
            index.drop()
        except:
            pass
        if keys:
            redis.delete(*keys)

    def test_query_hash_by_text(self, hash_index: IndexFixture):
        """Test querying hash index by text field."""
        index = hash_index["index"]
        result = index.query(filter={"name": {"$eq": "Alice"}})

        assert len(result) > 0

    def test_query_hash_with_sorting(self, hash_index: IndexFixture):
        """Test querying hash index with sorting."""
        index = hash_index["index"]
        result = index.query(filter={"score": {"$gte": 80}}, orderBy={"score": "DESC"})

        assert len(result) > 0


class TestNestedStringIndex:
    """Tests for nested string index queries."""

    @pytest.fixture(scope="class")
    def nested_string_index(self) -> Generator[IndexFixture, None, None]:
        """Create a nested string index with test data."""
        name = f"test-nested-string-{random_id()}"
        prefix = f"{name}:"
        keys = []

        redis = Redis.from_env()

        schema = {
            "title": "TEXT",
            "author": {"name": "TEXT", "email": "TEXT"},
            "stats": {"views": {"type": "U64", "fast": True}, "likes": {"type": "U64", "fast": True}},
        }

        index = redis.search.createIndex(
            name=name, schema=schema, dataType="string", prefix=prefix
        )

        test_data = [
            {
                "title": "First Post",
                "author": {"name": "John Doe", "email": "john@example.com"},
                "stats": {"views": 1000, "likes": 50},
            },
            {
                "title": "Second Post",
                "author": {"name": "Jane Smith", "email": "jane@example.com"},
                "stats": {"views": 500, "likes": 30},
            },
            {
                "title": "Third Post",
                "author": {"name": "John Doe", "email": "john@example.com"},
                "stats": {"views": 2000, "likes": 100},
            },
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            redis.set(key, json.dumps(datum))

        index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        # Cleanup
        try:
            index.drop()
        except:
            pass
        if keys:
            redis.delete(*keys)

    def test_query_nested_text_field(self, nested_string_index: IndexFixture):
        """Test querying nested text field."""
        index = nested_string_index["index"]
        result = index.query(filter={"author.name": {"$eq": "John"}})

        assert len(result) > 0

    def test_query_nested_numeric_field(self, nested_string_index: IndexFixture):
        """Test querying nested numeric field."""
        index = nested_string_index["index"]
        result = index.query(filter={"stats.views": {"$eq": 1000}})

        assert len(result) > 0

    def test_query_nested_with_sorting(self, nested_string_index: IndexFixture):
        """Test querying with sorting on nested field."""
        index = nested_string_index["index"]
        result = index.query(
            filter={"author.name": {"$eq": "John"}},
            select={"author.email": True},
            orderBy={"stats.views": "DESC"},
        )

        assert len(result) > 0


class TestNestedJsonIndex:
    """Tests for nested JSON index queries."""

    @pytest.fixture(scope="class")
    def nested_json_index(self) -> Generator[IndexFixture, None, None]:
        """Create a nested JSON index with test data."""
        name = f"test-nested-json-{random_id()}"
        prefix = f"{name}:"
        keys = []

        redis = Redis.from_env()

        schema = {
            "title": "TEXT",
            "author": {"name": "TEXT", "email": "TEXT"},
            "stats": {"views": {"type": "U64", "fast": True}, "likes": {"type": "U64", "fast": True}},
        }

        index = redis.search.createIndex(
            name=name, schema=schema, dataType="json", prefix=prefix
        )

        test_data = [
            {
                "title": "First Post",
                "author": {"name": "John Doe", "email": "john@example.com"},
                "stats": {"views": 1000, "likes": 50},
            },
            {
                "title": "Second Post",
                "author": {"name": "Jane Smith", "email": "jane@example.com"},
                "stats": {"views": 500, "likes": 30},
            },
            {
                "title": "Third Post",
                "author": {"name": "John Doe", "email": "john@example.com"},
                "stats": {"views": 2000, "likes": 100},
            },
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            redis.json.set(key, "$", datum)

        index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        # Cleanup
        try:
            index.drop()
        except:
            pass
        if keys:
            redis.delete(*keys)

    def test_query_nested_json_text_field(self, nested_json_index: IndexFixture):
        """Test querying nested text field in JSON index."""
        index = nested_json_index["index"]
        result = index.query(filter={"author.name": {"$eq": "John"}})

        assert len(result) > 0

    def test_query_nested_json_numeric_field(self, nested_json_index: IndexFixture):
        """Test querying nested numeric field in JSON index."""
        index = nested_json_index["index"]
        result = index.query(filter={"stats.views": {"$eq": 1000}})

        assert len(result) > 0

    def test_query_nested_json_with_sorting(self, nested_json_index: IndexFixture):
        """Test querying with sorting on nested field in JSON index."""
        index = nested_json_index["index"]
        result = index.query(
            filter={"author.name": {"$eq": "John"}},
            select={"author.email": True},
            orderBy={"stats.views": "DESC"},
        )

        assert len(result) > 0
