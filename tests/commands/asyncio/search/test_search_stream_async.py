"""Tests for search indexes over Redis streams, async client."""

import random
import string

from pytest import mark

from upstash_redis.asyncio import Redis
from upstash_redis.search import DataType


@mark.asyncio
async def test_stream_index_async(async_redis: Redis) -> None:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    name = f"test-stream-index-{suffix}"
    stream = f"test-stream-stream-{suffix}"

    index = await async_redis.search.create_index(
        name=name,
        schema={"message": "TEXT", "service": "KEYWORD"},
        data_type=DataType.STREAM,
        stream=stream,
    )
    try:
        await async_redis.xadd(stream, "1-0", {"message": "hello", "service": "web"})
        await index.wait_indexing()

        results = await index.query(filter={"message": "hello"})
        assert [r.key for r in results] == ["1-0"]

        description = await index.describe()
        assert description is not None
        assert description.data_type == DataType.STREAM
        assert description.prefixes == [stream]
    finally:
        await index.drop()
        await async_redis.delete(stream)
