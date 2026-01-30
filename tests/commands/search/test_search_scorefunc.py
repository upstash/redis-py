"""Tests for score function queries."""

import math
from typing import Generator, List, TypedDict

import pytest

from upstash_redis import Redis
from upstash_redis.commands import SearchIndexCommands


class ScoreFuncFixture(TypedDict):
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


class TestScoreFunctionQueries:
    """Tests for querying with score functions."""

    @pytest.fixture(scope="class")
    def scorefunc_index(self) -> Generator[ScoreFuncFixture, None, None]:
        """Create a test index with data for score function querying."""
        redis = Redis.from_env()
        name = f"test-scorefunc-{random_id()}"
        prefix = f"{name}:"
        keys = []

        try:
            # Create index
            index = redis.search.create_index(
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
                {"name": "Laptop Missing", "recency": "150"},
            ]

            for i, product in enumerate(products):
                key = f"{prefix}{i}"
                keys.append(key)
                redis.hset(key, values=product)

            # Wait for indexing
            index.wait_indexing()

            yield {"index": index, "redis": redis, "keys": keys, "name": name}

        finally:
            # Cleanup
            try:
                index.drop()
            except Exception:
                pass
            if keys:
                redis.delete(*keys)

    def test_query_with_simple_scorefunc(self, scorefunc_index: ScoreFuncFixture):
        """Test querying with simple scoreFunc that boosts by popularity."""
        index = scorefunc_index["index"]
        result = index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func={"field": "popularity", "scoreMode": "REPLACE"},
        )

        assert len(result) == 4
        # Scores should exactly match the popularity values
        scores_by_name = {r.data["name"]: r.score for r in result if r.data}
        assert scores_by_name["Laptop Air"] == pytest.approx(2000.0)
        assert scores_by_name["Laptop Pro"] == pytest.approx(1000.0)
        assert scores_by_name["Laptop Basic"] == pytest.approx(500.0)

    def test_query_with_scorefunc_using_modifier(self, scorefunc_index: ScoreFuncFixture):
        """Test querying with scoreFunc using modifier."""
        index = scorefunc_index["index"]
        result = index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func={
                "field": "popularity",
                "modifier": "LOG1P",
                "factor": 2,
                "scoreMode": "REPLACE",
            },
        )

        assert len(result) == 4
        scores_by_name = {r.data["name"]: r.score for r in result if r.data}
        assert scores_by_name["Laptop Air"] == pytest.approx(2 * math.log10(1 + 2000))
        assert scores_by_name["Laptop Pro"] == pytest.approx(2 * math.log10(1 + 1000))
        assert scores_by_name["Laptop Basic"] == pytest.approx(2 * math.log10(1 + 500))

    def test_query_with_scorefunc_using_scoremode_replace(
        self, scorefunc_index: ScoreFuncFixture
    ):
        """Test querying with scoreFunc using scoreMode replace."""
        index = scorefunc_index["index"]
        result = index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func={
                "field": "popularity",
                "scoreMode": "REPLACE",
            },
        )

        assert len(result) == 4
        scores_by_name = {r.data["name"]: r.score for r in result if r.data}
        assert scores_by_name["Laptop Air"] == pytest.approx(2000.0)
        assert scores_by_name["Laptop Pro"] == pytest.approx(1000.0)
        assert scores_by_name["Laptop Basic"] == pytest.approx(500.0)

    def test_query_with_multiple_field_values_combined(self, scorefunc_index: ScoreFuncFixture):
        """Test querying with multiple field values combined."""
        index = scorefunc_index["index"]
        result = index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func={
                "fields": [
                    {"field": "popularity", "modifier": "LOG1P"},
                    {"field": "recency", "modifier": "LOG1P"},
                ],
                "combineMode": "SUM",
                "scoreMode": "REPLACE",
            },
        )

        assert len(result) == 4
        scores_by_name = {r.data["name"]: r.score for r in result if r.data}
        assert scores_by_name["Laptop Air"] == pytest.approx(
            math.log10(1 + 2000) + math.log10(1 + 50)
        )
        assert scores_by_name["Laptop Pro"] == pytest.approx(
            math.log10(1 + 1000) + math.log10(1 + 100)
        )
        assert scores_by_name["Laptop Basic"] == pytest.approx(
            math.log10(1 + 500) + math.log10(1 + 200)
        )

    def test_query_with_scorefunc_modifier_and_missing(self, scorefunc_index: ScoreFuncFixture):
        """Test querying with scoreFunc using modifier and missing value."""
        index = scorefunc_index["index"]
        result = index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func={
                "field": "popularity",
                "modifier": "LOG1P",
                "missing": 1,
                "scoreMode": "REPLACE",
            },
        )

        assert len(result) == 4
        scores_by_name = {r.data["name"]: r.score for r in result if r.data}
        assert scores_by_name["Laptop Missing"] == pytest.approx(math.log10(1 + 1))

    def test_query_with_multiple_fields_and_scoremode(self, scorefunc_index: ScoreFuncFixture):
        """Test querying with multiple field values and scoreMode."""
        index = scorefunc_index["index"]
        result = index.query(
            filter={"name": {"$eq": "Laptop"}},
            score_func={
                "fields": ["popularity", "recency"],
                "combineMode": "MULTIPLY",
                "scoreMode": "REPLACE",
            },
        )

        assert len(result) == 4
        scores_by_name = {r.data["name"]: r.score for r in result if r.data}
        assert scores_by_name["Laptop Air"] == pytest.approx(2000 * 50)
        assert scores_by_name["Laptop Pro"] == pytest.approx(1000 * 100)
        assert scores_by_name["Laptop Basic"] == pytest.approx(500 * 200)
