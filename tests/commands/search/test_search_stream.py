"""Tests for search indexes over Redis streams."""

import random
import string
from typing import Callable, Generator, List

import pytest

from upstash_redis import Redis
from upstash_redis.commands import SearchIndexCommands
from upstash_redis.search import DataType, FieldType, Schema

SCHEMA: Schema = {
    "message": FieldType.TEXT,
    "severity": {"type": FieldType.U64, "fast": True},
    "service": FieldType.KEYWORD,
}


def random_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest.fixture
def names(redis: Redis) -> Generator[Callable[[str], str], None, None]:
    """Hands out unique names; drops created indexes and deletes streams afterwards."""
    indexes: List[str] = []
    streams: List[str] = []

    def factory(kind: str) -> str:
        name = f"test-stream-{kind}-{random_id()}"
        (indexes if kind == "index" else streams).append(name)
        return name

    yield factory

    for index in indexes:
        try:
            redis.search.index(index).drop()
        except Exception:
            pass
    if streams:
        redis.delete(*streams)


def test_stream_index_query(redis: Redis, names: Callable[[str], str]) -> None:
    stream = names("stream")
    index = redis.search.create_index(
        name=names("index"), schema=SCHEMA, data_type="STREAM", stream=stream
    )
    assert isinstance(index, SearchIndexCommands)

    redis.xadd(
        stream,
        "1-0",
        {
            "message": "Payment authorization failed",
            "severity": 4,
            "service": "checkout",
        },
    )
    redis.xadd(
        stream,
        "2-0",
        {"message": "Order completed", "severity": 1, "service": "checkout"},
    )
    index.wait_indexing()

    results = index.query(filter={"message": "authorization", "severity": {"$gte": 3}})
    assert len(results) == 1
    assert results[0].key == "1-0"
    assert results[0].data is not None
    assert results[0].data["message"] == "Payment authorization failed"
    assert results[0].data["service"] == "checkout"

    selected = index.query(
        filter={"service": "checkout"},
        order_by={"severity": "ASC"},
        select={"message": True},
    )
    assert [(r.key, r.data) for r in selected] == [
        ("2-0", {"message": "Order completed"}),
        ("1-0", {"message": "Payment authorization failed"}),
    ]

    assert index.count(filter={"service": "checkout"}).count == 2


def test_stream_index_describe(redis: Redis, names: Callable[[str], str]) -> None:
    stream = names("stream")
    name = names("index")
    index = redis.search.create_index(
        name=name, schema=SCHEMA, data_type=DataType.STREAM, stream=stream
    )

    description = index.describe()
    assert description is not None
    assert description.name == name
    assert description.data_type == DataType.STREAM
    assert description.prefixes == [stream]
    assert set(description.schema) == {"message", "severity", "service"}
    assert description.schema["severity"].fast is True


def test_stream_index_follows_deletions(
    redis: Redis, names: Callable[[str], str]
) -> None:
    stream = names("stream")
    index = redis.search.create_index(
        name=names("index"), schema=SCHEMA, data_type="stream", stream=stream
    )

    entry_id = redis.xadd(
        stream, "*", {"message": "bye", "severity": 2, "service": "api"}
    )
    index.wait_indexing()
    assert index.count(filter={"service": "api"}).count == 1

    redis.xdel(stream, entry_id)
    index.wait_indexing()
    assert index.count(filter={"service": "api"}).count == 0


def test_stream_index_skip_initial_scan(
    redis: Redis, names: Callable[[str], str]
) -> None:
    stream = names("stream")
    redis.xadd(stream, "1-0", {"message": "old", "severity": 1, "service": "a"})

    index = redis.search.create_index(
        name=names("index"),
        schema=SCHEMA,
        data_type="STREAM",
        stream=stream,
        skip_initial_scan=True,
    )
    redis.xadd(stream, "2-0", {"message": "new", "severity": 1, "service": "a"})
    index.wait_indexing()

    assert [r.key for r in index.query(filter={"service": "a"})] == ["2-0"]


def test_stream_index_argument_validation(redis: Redis) -> None:
    with pytest.raises(Exception, match='"stream" must be given'):
        redis.search.create_index(name="never", schema=SCHEMA, data_type="STREAM")

    with pytest.raises(Exception, match='"prefixes" cannot be used'):
        redis.search.create_index(
            name="never",
            schema=SCHEMA,
            data_type="STREAM",
            stream="events",
            prefixes="events:",
        )

    with pytest.raises(Exception, match='"prefixes" must be given'):
        redis.search.create_index(name="never", schema=SCHEMA, data_type="HASH")

    with pytest.raises(Exception, match='"stream" can only be used'):
        redis.search.create_index(
            name="never",
            schema=SCHEMA,
            data_type="HASH",
            prefixes="x:",
            stream="events",
        )
