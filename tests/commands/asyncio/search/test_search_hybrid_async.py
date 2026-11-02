"""Async tests for HYBRID fields and the $ann, $rrf and $standard retrievers."""

import random
import string

import pytest
import pytest_asyncio

from upstash_redis.asyncio import Redis
from upstash_redis.search import FieldType

PRODUCTS = [
    {
        "name": "Studio Headphones",
        "description": "Over-ear headphones with active noise cancelling",
        "category": "audio",
    },
    {
        "name": "Travel Earplugs",
        "description": "Soft foam earplugs that keep airplane cabin sound out",
        "category": "travel",
    },
    {
        "name": "Sport Earbuds",
        "description": "Sweat-proof wireless earbuds for running",
        "category": "sports",
    },
]


def random_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest_asyncio.fixture
async def hybrid_index():
    name = f"test-async-hybrid-{random_id()}"
    prefix = f"{name}:"
    redis = Redis.from_env()

    index = await redis.search.create_index(
        name=name,
        schema={
            "name": "TEXT",
            "description": FieldType.HYBRID,
            "category": "KEYWORD",
        },
        data_type="json",
        prefixes=prefix,
    )

    keys = []
    for i, product in enumerate(PRODUCTS):
        key = f"{prefix}{i + 1}"
        keys.append(key)
        await redis.json.set(key, "$", product)

    await index.wait_indexing()

    yield {"index": index, "prefix": prefix}

    try:
        await index.drop()
    except Exception:
        pass
    await redis.delete(*keys)


@pytest.mark.asyncio
async def test_ann_with_filter(hybrid_index) -> None:
    prefix = hybrid_index["prefix"]
    results = await hybrid_index["index"].query(
        filter={
            "$ann": {
                "field": "description",
                "query": "headphones for running",
                "filter": {"category": "sports"},
            }
        }
    )

    assert [r.key for r in results] == [f"{prefix}3"]


@pytest.mark.asyncio
async def test_rrf(hybrid_index) -> None:
    prefix = hybrid_index["prefix"]
    results = await hybrid_index["index"].query(
        filter={
            "$rrf": {
                "retrievers": [
                    {"description": "noise cancelling"},
                    {"$ann": {"field": "description", "query": "noise cancelling"}},
                ]
            }
        },
        limit=2,
    )

    assert len(results) == 2
    assert results[0].key == f"{prefix}1"
