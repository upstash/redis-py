"""Tests for HYBRID fields and the $ann, $rrf and $standard retrievers."""

import random
import string
from typing import Generator

import pytest

from upstash_redis import Redis
from upstash_redis.search import FieldType

PRODUCTS = [
    {
        "name": "Studio Headphones",
        "description": "Over-ear headphones with active noise cancelling",
        "category": "audio",
        "price": 199,
    },
    {
        "name": "Travel Earplugs",
        "description": "Soft foam earplugs that keep airplane cabin sound out",
        "category": "travel",
        "price": 9,
    },
    {
        "name": "Sport Earbuds",
        "description": "Sweat-proof wireless earbuds for running",
        "category": "sports",
        "price": 79,
    },
]


def random_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest.fixture(scope="module")
def hybrid_index() -> Generator[dict, None, None]:
    name = f"test-hybrid-{random_id()}"
    prefix = f"{name}:"
    redis = Redis.from_env()

    index = redis.search.create_index(
        name=name,
        schema={
            "name": "TEXT",
            "description": FieldType.HYBRID,
            "category": "KEYWORD",
            "price": {"type": "F64", "fast": True},
        },
        data_type="json",
        prefixes=prefix,
    )

    keys = []
    for i, product in enumerate(PRODUCTS):
        key = f"{prefix}{i + 1}"
        keys.append(key)
        redis.json.set(key, "$", product)

    index.wait_indexing()

    yield {"index": index, "redis": redis, "prefix": prefix}

    try:
        index.drop()
    except Exception:
        pass
    redis.delete(*keys)


def test_hybrid_field_in_describe(hybrid_index: dict) -> None:
    description = hybrid_index["index"].describe()
    assert description.schema["description"].type == FieldType.HYBRID


def test_hybrid_field_options(hybrid_index: dict) -> None:
    redis = hybrid_index["redis"]
    name = f"test-hybrid-opts-{random_id()}"
    index = redis.search.create_index(
        name=name,
        schema={"body": {"type": "HYBRID", "no_stem": True, "alias": "content"}},
        data_type="hash",
        prefixes=f"{name}:",
    )
    try:
        field = index.describe().schema["body"]
        assert field.type == "HYBRID"
        assert field.no_stem is True
        assert field.alias == "content"
    finally:
        index.drop()


def test_ann(hybrid_index: dict) -> None:
    results = hybrid_index["index"].query(
        filter={"$ann": {"field": "description", "query": "block noise on a flight"}}
    )

    assert len(results) == 3
    for result in results:
        assert 0 <= result.score <= 1


def test_ann_with_filter(hybrid_index: dict) -> None:
    prefix = hybrid_index["prefix"]
    results = hybrid_index["index"].query(
        filter={
            "$ann": {
                "field": "description",
                "query": "headphones for running",
                "profile": "precise",
                "filter": {"category": "sports"},
            }
        }
    )

    assert [r.key for r in results] == [f"{prefix}3"]
    assert results[0].data["name"] == "Sport Earbuds"


def test_hybrid_field_full_text(hybrid_index: dict) -> None:
    prefix = hybrid_index["prefix"]
    results = hybrid_index["index"].query(filter={"description": "earplugs"})
    assert [r.key for r in results] == [f"{prefix}2"]


def test_rrf(hybrid_index: dict) -> None:
    prefix = hybrid_index["prefix"]
    results = hybrid_index["index"].query(
        filter={
            "$rrf": {
                "retrievers": [
                    {
                        "$standard": {
                            "query": {"description": "noise cancelling"},
                            "weight": 2,
                        }
                    },
                    {"$ann": {"field": "description", "query": "noise cancelling"}},
                ],
                "window": 10,
            }
        },
        limit=2,
    )

    assert len(results) == 2
    assert results[0].key == f"{prefix}1"


def test_count_rrf(hybrid_index: dict) -> None:
    result = hybrid_index["index"].count(
        filter={
            "$rrf": {
                "retrievers": [{"description": "earplugs"}, {"name": "headphones"}]
            }
        }
    )
    assert result.count == 2
