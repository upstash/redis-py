"""Tests for search aggregation functionality."""

import json
from typing import Generator

import pytest

from upstash_redis import Redis


def random_id() -> str:
    import random
    import string

    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest.fixture
def redis_client() -> Redis:
    return Redis.from_env()


class TestAggregation:
    """Tests for aggregation queries."""

    @pytest.fixture(scope="class")
    def agg_index(self) -> Generator[dict, None, None]:
        name = f"test-agg-{random_id()}"
        prefix = f"{name}:"
        keys = []

        redis = Redis.from_env()

        schema = {
            "name": "TEXT",
            "category": "KEYWORD",
            "price": {"type": "F64", "fast": True},
            "stock": {"type": "U64", "fast": True},
            "active": "BOOL",
        }

        index = redis.search.create_index(
            name=name, schema=schema, data_type="string", prefixes=prefix
        )

        test_data = [
            {
                "name": "Laptop Pro",
                "category": "electronics",
                "price": 1299.99,
                "stock": 50,
                "active": True,
            },
            {
                "name": "Laptop Basic",
                "category": "electronics",
                "price": 599.99,
                "stock": 100,
                "active": True,
            },
            {
                "name": "Wireless Mouse",
                "category": "electronics",
                "price": 29.99,
                "stock": 200,
                "active": True,
            },
            {
                "name": "USB Cable",
                "category": "accessories",
                "price": 9.99,
                "stock": 500,
                "active": True,
            },
            {
                "name": "Phone Case",
                "category": "accessories",
                "price": 19.99,
                "stock": 300,
                "active": False,
            },
            {
                "name": "Keyboard",
                "category": "electronics",
                "price": 79.99,
                "stock": 150,
                "active": True,
            },
            {
                "name": "Monitor",
                "category": "electronics",
                "price": 399.99,
                "stock": 75,
                "active": True,
            },
            {
                "name": "Headphones",
                "category": "audio",
                "price": 149.99,
                "stock": 120,
                "active": True,
            },
            {
                "name": "Speaker",
                "category": "audio",
                "price": 59.99,
                "stock": 80,
                "active": True,
            },
        ]

        for i, datum in enumerate(test_data):
            key = f"{prefix}{i}"
            keys.append(key)
            redis.set(key, json.dumps(datum))

        index.wait_indexing()

        yield {"index": index, "redis": redis, "keys": keys, "name": name}

        try:
            index.drop()
        except Exception:
            pass
        if keys:
            redis.delete(*keys)

    def test_avg_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={"avg_price": {"$avg": {"field": "price"}}}
        )

        assert "avg_price" in result
        assert "value" in result["avg_price"]
        assert isinstance(result["avg_price"]["value"], (int, float))

    def test_sum_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={"total_price": {"$sum": {"field": "price"}}}
        )

        assert "total_price" in result
        assert "value" in result["total_price"]

    def test_min_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={"min_price": {"$min": {"field": "price"}}}
        )

        assert "min_price" in result
        assert "value" in result["min_price"]

    def test_max_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={"max_price": {"$max": {"field": "price"}}}
        )

        assert "max_price" in result
        assert "value" in result["max_price"]

    def test_count_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={"doc_count": {"$count": {"field": "price"}}}
        )

        assert "doc_count" in result
        assert "value" in result["doc_count"]

    def test_cardinality_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={"unique_stock": {"$cardinality": {"field": "stock"}}}
        )

        assert "unique_stock" in result
        assert "value" in result["unique_stock"]

    def test_stats_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={"price_stats": {"$stats": {"field": "price"}}}
        )

        assert "price_stats" in result
        stats = result["price_stats"]
        assert "count" in stats
        assert "min" in stats
        assert "max" in stats
        assert "sum" in stats
        assert "avg" in stats

    def test_extended_stats_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={"price_estats": {"$extendedStats": {"field": "price"}}}
        )

        assert "price_estats" in result
        estats = result["price_estats"]
        assert "count" in estats
        assert "min" in estats
        assert "max" in estats

    def test_percentiles_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={
                "price_pct": {
                    "$percentiles": {
                        "field": "price",
                        "percents": [50, 95],
                    }
                }
            }
        )

        assert "price_pct" in result

    def test_terms_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={"by_category": {"$terms": {"field": "category"}}}
        )

        assert "by_category" in result
        assert "buckets" in result["by_category"]
        buckets = result["by_category"]["buckets"]
        assert len(buckets) > 0
        for bucket in buckets:
            assert "key" in bucket
            assert "docCount" in bucket

    def test_range_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={
                "price_ranges": {
                    "$range": {
                        "field": "price",
                        "ranges": [
                            {"to": 50},
                            {"from": 50, "to": 500},
                            {"from": 500},
                        ],
                    }
                }
            }
        )

        assert "price_ranges" in result
        assert "buckets" in result["price_ranges"]

    def test_histogram_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={
                "price_hist": {
                    "$histogram": {
                        "field": "price",
                        "interval": 200,
                    }
                }
            }
        )

        assert "price_hist" in result
        assert "buckets" in result["price_hist"]

    def test_nested_sub_aggregation(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={
                "by_category": {
                    "$terms": {
                        "field": "category",
                    },
                    "$aggs": {
                        "avg_price": {"$avg": {"field": "price"}},
                    },
                }
            }
        )

        assert "by_category" in result
        assert "buckets" in result["by_category"]
        for bucket in result["by_category"]["buckets"]:
            assert "key" in bucket
            assert "avg_price" in bucket

    def test_combined_aggregations(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            aggregations={
                "avg_price": {"$avg": {"field": "price"}},
                "max_price": {"$max": {"field": "price"}},
                "doc_count": {"$count": {"field": "price"}},
            }
        )

        assert "avg_price" in result
        assert "max_price" in result
        assert "doc_count" in result

    def test_aggregate_with_filter(self, agg_index):
        index = agg_index["index"]
        result = index.aggregate(
            filter={"category": {"$eq": "electronics"}},
            aggregations={"avg_price": {"$avg": {"field": "price"}}},
        )

        assert "avg_price" in result
        assert "value" in result["avg_price"]
