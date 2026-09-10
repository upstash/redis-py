"""Tests for vector index commands (VECTOR.*)."""

import base64
import random
import string
import struct
from typing import Generator, List

import pytest

from upstash_redis import Redis
from upstash_redis.commands import VectorIndexCommands
from upstash_redis.vector import (
    VectorIndexInfo,
    VectorMetric,
    VectorQueryProfile,
    VectorQueryResult,
    serialize_vector,
)


def random_name() -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"test-vector-{suffix}"


@pytest.fixture
def index_names(redis: Redis) -> Generator[List[str], None, None]:
    names: List[str] = []
    yield names
    for name in names:
        redis.vector.index(name).drop()


def test_serialize_vector() -> None:
    assert serialize_vector([0.5, 1, 2]) == ["VALUES", 3, 0.5, 1.0, 2.0]
    assert serialize_vector((0.5,)) == ["VALUES", 1, 0.5]

    blob = struct.pack("<2f", 1.0, 0.0)
    encoded = base64.b64encode(blob).decode()
    assert serialize_vector(blob) == ["BASE64-FP32", encoded]
    assert serialize_vector(bytearray(blob)) == ["BASE64-FP32", encoded]
    assert serialize_vector(encoded) == ["BASE64-FP32", encoded]

    with pytest.raises(ValueError):
        serialize_vector(b"abc")


def test_create_add_get_query(redis: Redis, index_names: List[str]) -> None:
    name = random_name()
    index_names.append(name)

    index = redis.vector.create_index(name=name, dimension=3, metric="COSINE")
    assert isinstance(index, VectorIndexCommands)
    assert index.name == name

    assert index.add("a", [1, 0, 0]) == 1
    assert index.add("b", struct.pack("<3f", 0, 1, 0)) == 1
    assert index.add("a", [1, 0, 0]) == 0
    assert index.count() == 2

    assert index.get("a") == [1.0, 0.0, 0.0]
    assert index.get("missing") is None

    assert index.query(vector=[1, 0, 0], top_k=1) == [
        VectorQueryResult(id="a", score=1.0)
    ]


def test_query_orders_by_score_and_keeps_ids_as_strings(
    redis: Redis, index_names: List[str]
) -> None:
    name = random_name()
    index_names.append(name)
    index = redis.vector.create_index(
        name=name, dimension=3, metric=VectorMetric.COSINE
    )

    index.add("x", [1, 0, 0])
    index.add("123", [0.9, 0.1, 0])
    index.add("y", [0, 1, 0])

    results = index.query(
        vector=base64.b64encode(struct.pack("<3f", 1, 0, 0)).decode(),
        top_k=2,
        profile=VectorQueryProfile.PRECISE,
    )
    assert [r.id for r in results] == ["x", "123"]
    assert results[0].score >= results[1].score
    assert all(0 <= r.score <= 1 for r in results)


def test_get_returns_float32_precision(redis: Redis, index_names: List[str]) -> None:
    name = random_name()
    index_names.append(name)
    index = redis.vector.create_index(name=name, dimension=2, metric="euclidean")

    index.add("p", [0.1, 0.2])
    values = index.get("p")
    assert values is not None
    assert values == pytest.approx([0.1, 0.2], rel=1e-6)


def test_info_and_exists_ok(redis: Redis, index_names: List[str]) -> None:
    name = random_name()
    index_names.append(name)

    index = redis.vector.create_index(name=name, dimension=4, metric="DOT")
    assert index.info() == VectorIndexInfo(dimension=4, metric=VectorMetric.DOT)

    again = redis.vector.create_index(
        name=name, dimension=4, metric="DOT", exists_ok=True
    )
    assert again.name == name

    with pytest.raises(Exception):
        redis.vector.create_index(name=name, dimension=4, metric="DOT")


def test_delete_and_drop(redis: Redis) -> None:
    name = random_name()
    index = redis.vector.create_index(name=name, dimension=1, metric="COSINE")

    index.add("a", [1])
    assert index.delete("a") == 1
    assert index.delete("a") == 0
    assert index.count() == 0

    assert index.drop() == 1
    assert index.info() is None
    assert index.drop() == 0


def test_missing_index(redis: Redis) -> None:
    index = redis.vector.index(random_name())
    assert index.count() == 0
    assert index.info() is None
    with pytest.raises(Exception, match="not found"):
        index.add("a", [1, 2, 3])


def test_pipeline(redis: Redis, index_names: List[str]) -> None:
    name = random_name()
    index_names.append(name)

    pipeline = redis.pipeline()
    pipeline.vector.create_index(name=name, dimension=2, metric="COSINE")
    index = pipeline.vector.index(name)
    index.add("a", [1, 1])
    index.add("b", [1, 0])
    index.count()
    index.get("a")
    index.query(vector=[1, 1], top_k=1)
    index.info()
    created, added_a, added_b, count, vector, matches, info = pipeline.exec()

    assert isinstance(created, VectorIndexCommands)
    assert (added_a, added_b, count) == (1, 1, 2)
    assert vector == [1.0, 1.0]
    assert [m.id for m in matches] == ["a"]
    assert info == VectorIndexInfo(dimension=2, metric=VectorMetric.COSINE)


def test_transaction(redis: Redis) -> None:
    name = random_name()

    tx = redis.multi()
    tx.vector.create_index(name=name, dimension=2, metric="COSINE")
    tx.vector.index(name).add("a", [1, 0])
    tx.vector.index(name).count()
    tx.vector.index(name).drop()
    _, added, count, dropped = tx.exec()

    assert (added, count, dropped) == (1, 1, 1)
