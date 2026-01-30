"""Tests for search index functionality."""

import dataclasses
import json
from typing import List, Generator

import pytest

from upstash_redis import Redis
from upstash_redis.commands import SearchIndexCommands
from upstash_redis.search import Language, DataType, FieldType, Schema


@dataclasses.dataclass
class StringIndexFixture:
    index: SearchIndexCommands
    redis: Redis
    keys: List[str]
    name: str


@dataclasses.dataclass()
class CountIndexFixture:
    index: SearchIndexCommands
    redis: Redis
    keys: List[str]
    name: str


def random_id() -> str:
    """Generate a random ID for test isolation."""
    import random
    import string

    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest.fixture
def redis_client() -> Redis:
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
        except Exception:
            pass


class TestCreateIndex:
    """Tests for creating search indexes."""

    def test_create_string_index_simple_schema(
        self, redis_client: Redis, cleanup_indexes
    ):
        """Test creating a string index with simple schema."""
        name = f"test-string-{random_id()}"
        cleanup_indexes.append(name)

        schema: Schema = {"name": "TEXT", "age": {"type": "U64", "fast": True}}

        index = redis_client.search.create_index(
            name=name, schema=schema, data_type="string", prefixes=f"{name}:"
        )

        assert index is not None

    def test_create_string_index_nested_schema(
        self, redis_client: Redis, cleanup_indexes
    ):
        """Test creating a string index with nested schema."""
        name = f"test-string-nested-{random_id()}"
        cleanup_indexes.append(name)

        schema: Schema = {
            "title": "TEXT",
            "metadata.author": "TEXT",
            "metadata.views": {"type": "U64", "fast": True},
        }

        index = redis_client.search.create_index(
            name=name, schema=schema, data_type="string", prefixes=f"{name}:"
        )

        assert index is not None

    def test_create_hash_index(self, redis_client: Redis, cleanup_indexes):
        """Test creating a hash index."""
        name = f"test-hash-{random_id()}"
        cleanup_indexes.append(name)

        schema: Schema = {"title": "TEXT", "count": {"type": "U64", "fast": True}}

        index = redis_client.search.create_index(
            name=name, schema=schema, data_type="hash", prefixes=f"{name}:"
        )

        assert index is not None

    def test_create_json_index_simple_schema(
        self, redis_client: Redis, cleanup_indexes
    ):
        """Test creating a JSON index with simple schema."""
        name = f"test-json-{random_id()}"
        cleanup_indexes.append(name)

        schema: Schema = {"name": "TEXT", "age": {"type": "U64", "fast": True}}

        index = redis_client.search.create_index(
            name=name, schema=schema, data_type="json", prefixes=f"{name}:"
        )

        assert index is not None

    def test_create_json_index_nested_schema(
        self, redis_client: Redis, cleanup_indexes
    ):
        """Test creating a JSON index with nested schema."""
        name = f"test-json-nested-{random_id()}"
        cleanup_indexes.append(name)

        schema: Schema = {
            "title": "TEXT",
            "metadata.author": "TEXT",
            "metadata.views": {"type": "U64", "fast": True},
        }

        index = redis_client.search.create_index(
            name=name, schema=schema, data_type="json", prefixes=f"{name}:"
        )

        assert index is not None

    def test_create_index_with_language(self, redis_client: Redis, cleanup_indexes):
        """Test creating an index with language option."""
        name = f"test-lang-{random_id()}"
        cleanup_indexes.append(name)

        schema: Schema = {"content": "TEXT"}

        index = redis_client.search.create_index(
            name=name,
            schema=schema,
            data_type="string",
            prefixes=f"{name}:",
            language="turkish",
        )

        assert index is not None

    def test_create_index_with_multiple_prefixes(
        self, redis_client: Redis, cleanup_indexes
    ):
        """Test creating an index with multiple prefixes."""
        name = f"test-multi-prefix-{random_id()}"
        cleanup_indexes.append(name)

        schema: Schema = {"name": "TEXT"}

        index = redis_client.search.create_index(
            name=name,
            schema=schema,
            data_type="hash",
            prefixes=[f"{name}:users:", f"{name}:profiles:"],
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
            "category": {"type": "TEXT", "no_tokenize": True},
            "price": {"type": "F64", "fast": True},
            "stock": {"type": "U64", "fast": True},
            "active": "BOOL",
        }

        index = redis.search.create_index(
            name=name, schema=schema, data_type="string", prefixes=prefix
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

        yield StringIndexFixture(index=index, redis=redis, keys=keys, name=name)

        # Cleanup
        try:
            index.drop()
        except Exception:
            pass
        if keys:
            redis.delete(*keys)

    def test_query_text_eq(self, string_index: StringIndexFixture):
        """Test querying with $eq on text field."""
        index = string_index.index
        result = index.query(filter={"name": {"$eq": "Laptop"}})

        # Should find 2 laptops: Laptop Pro and Laptop Basic
        assert len(result) == 2
        for r in result:
            assert r.key.startswith(string_index.name)
            assert r.score > 0.0

    def test_query_text_fuzzy(self, string_index: StringIndexFixture):
        """Test querying with fuzzy search for typo tolerance."""
        index = string_index.index
        result = index.query(filter={"name": {"$fuzzy": "laptopp"}})

        # Fuzzy search should find Laptop variants despite typo
        assert len(result) >= 2
        for r in result:
            assert r.key.startswith(string_index.name)
            assert r.score > 0.0

    def test_query_text_smart(self, string_index: StringIndexFixture):
        """Test querying with smart search for intelligent matching."""
        index = string_index.index
        result = index.query(filter={"name": {"$smart": "lapto"}})

        # Smart search should find Laptop variants with partial term
        assert len(result) >= 2
        for r in result:
            assert r.key.startswith(string_index.name)
            assert r.score > 0.0

    def test_query_text_phrase(self, string_index: StringIndexFixture):
        """Test querying with phrase matching."""
        index = string_index.index
        result = index.query(filter={"description": {"$phrase": "wireless mouse"}})

        # Should find exactly the Wireless Mouse item
        assert len(result) == 1
        # Check description field in data
        assert result[0].data is not None
        assert "description" in result[0].data and "name" in result[0].data

    def test_query_text_regex(self, string_index: StringIndexFixture):
        """Test querying with regex pattern."""
        index = string_index.index
        result = index.query(filter={"name": {"$regex": "Laptop.*"}})

        # Should find both Laptop Pro and Laptop Basic
        assert len(result) == 2

    def test_query_numeric_gt(self, string_index: StringIndexFixture):
        """Test querying with $gt on numeric field."""
        index = string_index.index
        result = index.query(filter={"price": {"$gt": 500}})

        # Should find Laptop Pro (1299.99) and Laptop Basic (599.99)
        assert len(result) == 2
        for r in result:
            assert r.data is not None
            assert "price" in r.data
            # Verify price is greater than 500
            price_value = r.data["price"]
            assert price_value > 500

    def test_query_numeric_gte(self, string_index: StringIndexFixture):
        """Test querying with $gte on numeric field."""
        index = string_index.index
        result = index.query(filter={"stock": {"$gte": 100}})

        # Should find items with stock >= 100 (Laptop Basic: 100, Mouse: 200, Cable: 500, Case: 300)
        assert len(result) == 4

    def test_query_numeric_lt(self, string_index: StringIndexFixture):
        """Test querying with $lt on numeric field."""
        index = string_index.index
        result = index.query(filter={"price": {"$lt": 50}})

        # Should find Wireless Mouse (29.99), USB Cable (9.99), Phone Case (19.99)
        assert len(result) == 3

    def test_query_numeric_lte(self, string_index: StringIndexFixture):
        """Test querying with $lte on numeric field."""
        index = string_index.index
        result = index.query(filter={"stock": {"$lte": 50}})

        # Should find only Laptop Pro (stock: 50)
        assert len(result) == 1
        assert result[0].data is not None
        assert "stock" in result[0].data
        assert result[0].data["stock"] <= 50

    def test_query_boolean_eq(self, string_index: StringIndexFixture):
        """Test querying with $eq on boolean field."""
        index = string_index.index
        result = index.query(filter={"active": {"$eq": False}})

        # Should find only Phone Case (active: False)
        assert len(result) == 1
        assert result[0].data is not None
        assert "active" in result[0].data
        # Verify active is False
        assert result[0].data["active"] is False

    def test_query_with_limit(self, string_index: StringIndexFixture):
        """Test querying with limit option."""
        index = string_index.index
        result = index.query(filter={"category": {"$eq": "electronics"}}, limit=2)

        # Should limit to exactly 2 results even though 3 match
        assert len(result) == 2

    def test_query_with_pagination(self, string_index: StringIndexFixture):
        """Test querying with offset for pagination."""
        index = string_index.index
        first_page = index.query(
            filter={"category": {"$eq": "electronics"}}, limit=2, offset=0
        )

        second_page = index.query(
            filter={"category": {"$eq": "electronics"}}, limit=2, offset=2
        )

        # Should get 2 items on first page
        assert len(first_page) == 2
        # Should get 1 item on second page (3 total electronics)
        assert len(second_page) == 1
        # Pages should have different items
        first_keys = {r.key for r in first_page}
        second_keys = {r.key for r in second_page}
        assert first_keys.isdisjoint(second_keys)

    def test_query_with_sort_asc(self, string_index: StringIndexFixture):
        """Test querying with sortBy ascending."""
        index = string_index.index
        result = index.query(
            filter={"category": {"$eq": "electronics"}},
            order_by={"price": "ASC"},
            limit=3,
        )

        # Should get all 3 electronics items sorted by price
        assert len(result) == 3
        prev_score = float("-inf")
        for r in result:
            assert r.score > prev_score
            prev_score = r.score

    def test_query_with_sort_desc(self, string_index: StringIndexFixture):
        """Test querying with sortBy descending."""
        index = string_index.index
        result = index.query(
            filter={"category": {"$eq": "electronics"}},
            order_by={"price": "DESC"},
            limit=3,
        )

        # Should get all 3 electronics items sorted by price descending
        assert len(result) == 3
        prev_score = float("inf")
        for r in result:
            assert r.score < prev_score
            prev_score = r.score

    def test_query_with_all_fields(self, string_index: StringIndexFixture):
        """Test querying returns all fields when select is not specified."""
        index = string_index.index
        result = index.query(filter={"name": {"$eq": "Wireless Mouse"}}, limit=1)

        # Should get complete result with all fields
        assert len(result) == 1
        item = result[0]

        # Verify all fields are present in data (values are strings for string data_type)
        data = item.data
        assert data is not None
        assert data["name"] == "Wireless Mouse"
        assert data["description"] == "Ergonomic wireless mouse"
        assert data["category"] == "electronics"
        assert data["price"] == 29.99
        assert data["stock"] == 200
        assert data["active"] is True

    def test_query_no_content(self, string_index: StringIndexFixture):
        """Test querying with noContent (keys only)."""
        index = string_index.index
        result = index.query(
            filter={"name": {"$eq": "Wireless Mouse"}}, select={}, limit=1
        )

        # Should get only key and score, no data field
        assert len(result) == 1
        item = result[0]

        # With noContent, data field should not be present
        assert item.data is None

    def test_query_with_return_fields(self, string_index: StringIndexFixture):
        """Test querying with specific return fields."""
        index = string_index.index
        result = index.query(
            filter={"name": {"$eq": "Wireless Mouse"}},
            select={"price": True, "stock": True},
            limit=1,
        )

        # Should get only specified fields
        assert len(result) == 1
        item = result[0]

        # Verify only selected fields are present (values are strings for string data_type)
        data = item.data
        assert data is not None
        assert "price" in data
        assert data["price"] == "29.99"
        assert "stock" in data
        assert data["stock"] == "200"
        # Other fields should not be present
        assert "name" not in data
        assert "description" not in data
        assert "category" not in data
        assert "active" not in data


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

        index = redis.search.create_index(
            name=name, schema=schema, data_type="string", prefixes=prefix
        )

        # Add test data
        for i in range(10):
            key = f"{prefix}{i}"
            keys.append(key)
            redis.set(key, json.dumps({"type": "A" if i < 5 else "B", "value": i}))

        index.wait_indexing()

        yield CountIndexFixture(index=index, redis=redis, keys=keys, name=name)

        # Cleanup
        try:
            index.drop()
        except Exception:
            pass
        if keys:
            redis.delete(*keys)

    def test_count_matching_documents(self, count_index: CountIndexFixture):
        """Test counting matching documents."""
        index = count_index.index
        result = index.count(filter={"type": {"$eq": "A"}})

        # Should count exactly 5 documents of type A (indices 0-4)
        assert result.count == 5

    def test_count_with_numeric_filter(self, count_index: CountIndexFixture):
        """Test counting with numeric filter."""
        index = count_index.index
        result = index.count(filter={"value": {"$eq": 5}})

        # Should count exactly 1 document with value 5
        assert result.count == 1


class TestDescribe:
    """Tests for describe functionality."""

    def test_describe_index(self, redis_client: Redis, cleanup_indexes):
        """Test describing an index structure."""
        name = f"test-describe-{random_id()}"
        cleanup_indexes.append(name)

        schema: Schema = {
            "title": {"type": "TEXT", "no_stem": True},
            "count": {"type": "U64", "fast": True},
            "active": "BOOL",
        }

        index = redis_client.search.create_index(
            name=name, schema=schema, data_type="string", prefixes=f"{name}:"
        )

        description = index.describe()

        assert description is not None
        assert description.name == name
        assert description.language == Language.ENGLISH
        assert description.data_type == DataType.STRING

        assert description.schema["active"].type == FieldType.BOOL
        assert description.schema["title"].type == FieldType.TEXT
        assert description.schema["title"].no_stem is True
        assert description.schema["count"].type == FieldType.U64
        assert description.schema["count"].fast is True


class TestDrop:
    """Tests for drop functionality."""

    def test_drop_existing_index(self, redis_client: Redis):
        """Test dropping an existing index."""
        name = f"test-drop-{random_id()}"

        schema: Schema = {"name": "TEXT"}

        index = redis_client.search.create_index(
            name=name, schema=schema, data_type="string", prefixes=f"{name}:"
        )

        index.drop()


class TestIndexMethod:
    """Tests for redis.search.index method."""

    def test_index(self, redis_client: Redis):
        """Test creating a SearchIndex instance without schema."""
        index = redis_client.search.index("test-index")

        assert index is not None
