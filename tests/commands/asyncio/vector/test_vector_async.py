"""Tests for vector index commands (VECTOR.*), async client."""

import random
import string
import struct

import pytest
from pytest import mark

from upstash_redis.asyncio import Redis
from upstash_redis.commands import VectorIndexCommands
from upstash_redis.vector import VectorIndexInfo, VectorMetric, VectorQueryResult


def random_name() -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"test-vector-{suffix}"


@mark.asyncio
async def test_vector_index_lifecycle(async_redis: Redis) -> None:
    name = random_name()
    index = await async_redis.vector.create_index(
        name=name, dimension=3, metric="COSINE"
    )
    try:
        assert isinstance(index, VectorIndexCommands)
        assert await index.info() == VectorIndexInfo(
            dimension=3, metric=VectorMetric.COSINE
        )

        assert await index.add("a", [1, 0, 0]) == 1
        assert await index.add("b", struct.pack("<3f", 0, 1, 0)) == 1
        assert await index.count() == 2
        assert await index.get("b") == [0.0, 1.0, 0.0]
        assert await index.get("missing") is None

        assert await index.query(vector=[1, 0, 0], top_k=1, profile="FAST") == [
            VectorQueryResult(id="a", score=1.0)
        ]

        same = async_redis.vector.index(name)
        assert await same.delete("a") == 1
        assert await same.count() == 1
    finally:
        assert await index.drop() == 1

    assert await index.info() is None


@mark.asyncio
async def test_vector_pipeline(async_redis: Redis) -> None:
    name = random_name()

    pipeline = async_redis.pipeline()
    pipeline.vector.create_index(name=name, dimension=2, metric="COSINE")
    pipeline.vector.index(name).add("a", [1, 1])
    pipeline.vector.index(name).query(vector=[1, 1], top_k=1)
    pipeline.vector.index(name).drop()
    _, added, matches, dropped = await pipeline.exec()

    assert added == 1
    assert isinstance(matches, list)
    assert [m.id for m in matches] == ["a"]
    assert dropped == 1


@mark.asyncio
async def test_vector_missing_index_raises(async_redis: Redis) -> None:
    with pytest.raises(Exception, match="not found"):
        await async_redis.vector.index(random_name()).get("a")
