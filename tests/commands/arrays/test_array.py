"""Tests for array commands (AR*)."""

import random
import string
from typing import Callable, Generator, List

import pytest

from upstash_redis import Redis


@pytest.fixture
def new_key(redis: Redis) -> Generator[Callable[[], str], None, None]:
    keys: List[str] = []

    def factory() -> str:
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        key = f"test-array-{suffix}"
        keys.append(key)
        return key

    yield factory

    if keys:
        redis.delete(*keys)


def test_arset_and_reads(redis: Redis, new_key: Callable[[], str]) -> None:
    key = new_key()

    assert redis.arset(key, 0, "a", "b") == 2
    assert redis.arset(key, 1, "B", "c") == 1
    assert redis.arget(key, 1) == "B"
    assert redis.arget(key, 99) is None
    assert redis.arget(new_key(), 0) is None

    assert redis.argetrange(key, 0, 3) == ["a", "B", "c", None]
    assert redis.argetrange(key, 2, 0) == ["c", "B", "a"]


def test_armset_and_armget(redis: Redis, new_key: Callable[[], str]) -> None:
    key = new_key()

    assert redis.armset(key, {0: "a", 100: "b"}) == 2
    assert redis.armset(key, [(100, "B"), (5, "c")]) == 1
    assert redis.armget(key, 100, 50, 0) == ["B", None, "a"]
    assert redis.armget(new_key(), 0, 1) == [None, None]


def test_arscan(redis: Redis, new_key: Callable[[], str]) -> None:
    key = new_key()
    redis.armset(key, {10: "a", 1000: "b", 5000: "c"})

    assert redis.arscan(key, 0, 10_000) == [(10, "a"), (1000, "b"), (5000, "c")]
    assert redis.arscan(key, 0, 10_000, limit=2) == [(10, "a"), (1000, "b")]


def test_argrep(redis: Redis, new_key: Callable[[], str]) -> None:
    key = new_key()
    redis.arset(key, 0, "error: disk", "ok", "ERROR: net", "apricot")

    assert redis.argrep(key, "-", "+", match="error") == [0]
    assert redis.argrep(key, 0, 10, match="error", nocase=True) == [0, 2]
    assert redis.argrep(key, "-", "+", exact=["ok", "apricot"]) == [1, 3]
    assert redis.argrep(key, "-", "+", regex="^[a-z]+$", withvalues=True) == [
        (1, "ok"),
        (3, "apricot"),
    ]
    assert redis.argrep(
        key, "-", "+", glob="a*", match="cot", combine="AND", withvalues=True
    ) == [(3, "apricot")]
    assert redis.argrep(key, "-", "+", match="o", limit=1) == [0]

    with pytest.raises(Exception):
        redis.argrep(key, "-", "+")


def test_ardel_ardelrange_arcount_arlen(
    redis: Redis, new_key: Callable[[], str]
) -> None:
    key = new_key()
    redis.arset(key, 0, "a", "b", "c", "d", "e")

    assert redis.ardel(key, 1, 1, 99) == 1
    assert redis.arcount(key) == 4
    assert redis.arlen(key) == 5

    assert redis.ardelrange(key, (0, 2), (4, 4)) == 3
    assert redis.arcount(key) == 1

    sparse = new_key()
    redis.arset(sparse, 1000, "x")
    assert redis.arlen(sparse) == 1001
    assert redis.arcount(sparse) == 1

    assert redis.arcount(new_key()) == 0
    assert redis.arlen(new_key()) == 0


def test_arinsert_arnext_arseek(redis: Redis, new_key: Callable[[], str]) -> None:
    key = new_key()

    assert redis.arnext(key) == 0
    assert redis.arinsert(key, "a", "b") == 1
    assert redis.arinsert(key, "c") == 2
    assert redis.arnext(key) == 3

    # deleting does not rewind the cursor, and ARSET does not move it
    redis.ardel(key, 2)
    redis.arset(key, 50, "x")
    assert redis.arinsert(key, "d") == 3

    assert redis.arseek(key, 10) is True
    assert redis.arnext(key) == 10
    assert redis.arinsert(key, "z") == 10
    assert redis.arseek(new_key(), 5) is False


def test_arring_and_arlastitems(redis: Redis, new_key: Callable[[], str]) -> None:
    key = new_key()

    assert redis.arring(key, 3, "a", "b", "c") == 2
    assert redis.arring(key, 3, "d") == 0
    assert redis.arlastitems(key, 3) == ["b", "c", "d"]
    assert redis.arlastitems(key, 3, rev=True) == ["d", "c", "b"]


def test_arop(redis: Redis, new_key: Callable[[], str]) -> None:
    key = new_key()
    redis.arset(key, 0, 1, 2, "x", 4.5)

    assert redis.arop(key, 0, 3, "SUM") == 7.5
    assert redis.arop(key, 0, 3, "min") == 1
    assert redis.arop(key, 0, 1, "MAX") == 2
    assert redis.arop(key, 0, 1, "XOR") == 3
    assert redis.arop(key, 0, 3, "USED") == 4
    assert redis.arop(key, 0, 3, "MATCH", "x") == 1
    assert redis.arop(key, 2, 2, "SUM") is None
    assert redis.arop(key, 50, 60, "USED") == 0

    with pytest.raises(Exception):
        redis.arop(key, 0, 3, "MATCH")
    with pytest.raises(Exception):
        redis.arop(key, 0, 3, "SUM", "x")


def test_arinfo(redis: Redis, new_key: Callable[[], str]) -> None:
    key = new_key()
    redis.arinsert(key, "a", "b", "c")

    info = redis.arinfo(key)
    assert info["len"] == 3
    assert info["count"] == 3
    assert info["next_insert_index"] == 3
    assert "dense_slices" not in info

    full = redis.arinfo(key, full=True)
    assert isinstance(full["dense_slices"], int)

    with pytest.raises(Exception):
        redis.arinfo(new_key())


def test_array_pipeline(redis: Redis, new_key: Callable[[], str]) -> None:
    key = new_key()

    pipeline = redis.pipeline()
    pipeline.arinsert(key, "a", "b")
    pipeline.arscan(key, 0, 10)
    pipeline.argrep(key, "-", "+", exact="b", withvalues=True)
    pipeline.arop(key, 0, 10, "USED")
    pipeline.arseek(key, 5)
    assert pipeline.exec() == [1, [(0, "a"), (1, "b")], [(1, "b")], 2, True]
