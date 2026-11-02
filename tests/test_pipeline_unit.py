from typing import Literal
from unittest.mock import AsyncMock, Mock

import pytest

from upstash_redis.client import Pipeline
from upstash_redis.asyncio.client import AsyncPipeline
from upstash_redis.http import AsyncHttpClient, SyncHttpClient


@pytest.mark.parametrize("mode", ["pipeline", "multi-exec"])
@pytest.mark.parametrize("pipeline_class", [Pipeline, AsyncPipeline])
def test_pipeline_length_tracks_queued_commands(pipeline_class, mode) -> None:
    pipeline = pipeline_class(
        url="https://example.invalid",
        headers={},
        http=Mock(),
        multi_exec=mode,
        set_sync_token_header_fn=Mock(),
        original_client=None,
    )
    assert len(pipeline) == 0
    assert not pipeline
    pipeline.set("key", "value").get("key")
    assert len(pipeline) == 2
    assert pipeline
    pipeline.reset()
    assert len(pipeline) == 0
    assert not pipeline


@pytest.mark.parametrize("mode", ["pipeline", "multi-exec"])
def test_empty_pipeline_does_not_send_request(
    mode: Literal["pipeline", "multi-exec"],
) -> None:
    http = Mock(spec=SyncHttpClient)
    update_headers = Mock()
    pipeline = Pipeline(
        url="https://example.invalid",
        headers={},
        http=http,
        multi_exec=mode,
        set_sync_token_header_fn=update_headers,
        original_client=None,
    )
    assert pipeline.exec() == []
    http.execute.assert_not_called()
    update_headers.assert_not_called()
    http.execute.return_value = ["OK", "value"]
    pipeline.set("key", "value").get("key")
    assert pipeline.exec() == [True, "value"]
    http.execute.assert_called_once()
    assert len(pipeline) == 0
    assert pipeline.exec() == []
    http.execute.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["pipeline", "multi-exec"])
async def test_empty_async_pipeline_does_not_send_request(
    mode: Literal["pipeline", "multi-exec"],
) -> None:
    http = Mock(spec=AsyncHttpClient)
    http.execute = AsyncMock(return_value=["OK", "value"])
    update_headers = Mock()
    pipeline = AsyncPipeline(
        url="https://example.invalid",
        headers={},
        http=http,
        multi_exec=mode,
        set_sync_token_header_fn=update_headers,
        original_client=None,
    )
    assert await pipeline.exec() == []
    http.execute.assert_not_awaited()
    update_headers.assert_not_called()
    pipeline.set("key", "value").get("key")
    assert await pipeline.exec() == [True, "value"]
    http.execute.assert_awaited_once()
    assert len(pipeline) == 0
    assert await pipeline.exec() == []
    http.execute.assert_awaited_once()
