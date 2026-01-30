"""Tests for async score function queries."""

import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis as AsyncRedis


def random_id() -> str:
    """Generate a random ID for test isolation."""
    import random
    import string

    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest.fixture
def async_redis_client() -> AsyncRedis:
    """Create an async Redis client for testing."""
    return AsyncRedis.from_env()


@pytest.fixture
def cleanup_indexes():
    """Track and cleanup indexes created during tests."""
    indexes = []

    yield indexes

    # Cleanup after test
    import asyncio

    async def cleanup():
        redis = AsyncRedis.from_env()
        for index_name in indexes:
            try:
                index = redis.search.index(index_name)
                await index.drop()
            except Exception:
                pass

    asyncio.run(cleanup())


@pytest.mark.asyncio
class TestAsyncScoreFunctionQueries:
    """Tests for async querying with score functions."""

    @pytest_asyncio.fixture(scope="class")
    async def scorefunc_index(self) -> dict:
        """Create a test index with data for score function querying."""
        redis = AsyncRedis.from_env()
        name = f"test-scorefunc-async-{random_id()}"
        prefix = f"{name}:"
        keys = []

        try:
            # Create index
            index = await redis.search.create_index(
                name=name,
                schema={
                    "name": "TEXT",
                    "popularity": {"type": "U64", "fast": True},
                    "recency": {"type": "U64", "fast": True},
                },
                data_type="hash",
                prefixes=prefix,
            )

            # Add test data with different popularity scores
            products = [
                {"name": "Laptop Pro", "popularity": "1000", "recency": "100"},
                {"name": "Laptop Basic", "popularity": "500", "recency": "200"},
                {"name": "Laptop Air", "popularity": "2000", "recency": "50"},
            ]

            for i, product in enumerate(products):
                key = f"{prefix}{i}"
                keys.append(key)
                await redis.hset(key, values=product)

            # Wait for indexing
            await index.wait_indexing()

            yield {"index": index, "redis": redis, "keys": keys, "name": name}

        finally:
            # Cleanup
            try:
                await index.drop()
            except Exception:
                pass
            if keys:
                await redis.delete(*keys)

    async def test_async_query_with_simple_scorefunc(self, scorefunc_index: dict):
        """Test async querying with simple scoreFunc that boosts by popularity."""
        index = scorefunc_index["index"]
        result = await index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func="popularity",
        )

        assert len(result) == 3
        # Higher popularity should result in higher scores
        assert result[0].data is not None
        assert result[0].data["name"] == "Laptop Air"  # popularity 2000

    async def test_async_query_with_scorefunc_using_modifier(
        self, scorefunc_index: dict
    ):
        """Test async querying with scoreFunc using modifier."""
        index = scorefunc_index["index"]
        result = await index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func={
                "field": "popularity",
                "modifier": "LOG1P",
                "factor": 2,
            },
        )

        assert len(result) == 3
        # Results should be affected by log1p(popularity) * 2.0
        assert result[0].data is not None
        assert result[0].data["name"] == "Laptop Air"

    async def test_async_query_with_scorefunc_using_scoremode_replace(
        self, scorefunc_index: dict
    ):
        """Test async querying with scoreFunc using scoreMode replace."""
        index = scorefunc_index["index"]
        result = await index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func={
                "field": "popularity",
                "scoreMode": "REPLACE",
            },
        )

        assert len(result) == 3
        # Scores should be replaced with popularity values
        assert result[0].data is not None
        assert result[0].data["name"] == "Laptop Air"  # popularity 2000
        assert result[0].score > result[1].score

    async def test_async_query_with_multiple_field_values_combined(
        self, scorefunc_index: dict
    ):
        """Test async querying with multiple field values combined."""
        index = scorefunc_index["index"]
        result = await index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func={
                "fields": [
                    {"field": "popularity", "modifier": "LOG1P"},
                    {"field": "recency", "modifier": "LOG1P"},
                ],
                "combineMode": "SUM",
            },
        )

        assert len(result) == 3
        # Results should be affected by log1p(popularity) + log1p(recency)
        assert result[0].data is not None
