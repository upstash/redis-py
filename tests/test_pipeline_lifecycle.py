import asyncio
import json
from unittest.mock import patch

import httpx
import pytest

from upstash_redis import Redis
from upstash_redis.asyncio import Redis as AsyncRedis
from upstash_redis.errors import UpstashError


@pytest.fixture(params=["pipeline", "multi"])
def batch_method(request):
    return request.param


@pytest.fixture(params=["redis_first", "redis_last", "cast", "transport", "json"])
def failed_batch(request):
    failure = request.param
    requests = []
    counter = 0
    transport_error = httpx.ReadError("response lost")

    def handle(http_request):
        nonlocal counter
        commands = json.loads(http_request.content)
        requests.append(commands)
        assert http_request.url.path in ("/pipeline", "/multi-exec")
        if len(requests) == 1:
            counter += sum(command[0] == "INCR" for command in commands)
            if failure == "transport":
                raise transport_error
            if failure == "json":
                return httpx.Response(200, content="{")
            return httpx.Response(
                200,
                json=[
                    {"result": counter}
                    if command[0] == "INCR"
                    else {"result": "{"}
                    if failure == "cast"
                    else {"error": "WRONGTYPE"}
                    for command in commands
                ],
            )
        assert commands == [["GET", "counter"]]
        assert counter == 1
        return httpx.Response(200, json=[{"result": str(counter)}])

    def queue(batch):
        if failure == "redis_first":
            batch.hget("wrong_type", "field").incr("counter")
        elif failure == "cast":
            batch.incr("counter").json.get("invalid_json")
        else:
            batch.incr("counter").hget("wrong_type", "field")

    error = (
        httpx.ReadError
        if failure == "transport"
        else json.JSONDecodeError
        if failure in ("cast", "json")
        else UpstashError
    )
    return httpx.MockTransport(handle), queue, error, requests, transport_error


def test_failed_batch_is_consumed(batch_method, failed_batch):
    transport, queue, error, requests, transport_error = failed_batch
    with httpx.Client(transport=transport) as http:
        with patch("upstash_redis.http.httpx.Client", return_value=http):
            with Redis(
                url="https://example.test",
                token="token",
                rest_encoding=None,
                rest_retries=0,
            ) as redis:
                batch = getattr(redis, batch_method)()
                queue(batch)
                with pytest.raises(error) as raised:
                    batch.exec()
                if error is httpx.ReadError:
                    assert raised.value is transport_error
                elif error is UpstashError:
                    assert str(raised.value) == "WRONGTYPE"
                assert batch._command_stack == []
                batch.get("counter")
                assert batch.exec() == ["1"]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_async_failed_batch_is_consumed(batch_method, failed_batch):
    transport, queue, error, requests, transport_error = failed_batch
    async with httpx.AsyncClient(transport=transport) as http:
        with patch("upstash_redis.http.httpx.AsyncClient", return_value=http):
            async with AsyncRedis(
                url="https://example.test",
                token="token",
                rest_encoding=None,
                rest_retries=0,
            ) as redis:
                batch = getattr(redis, batch_method)()
                queue(batch)
                with pytest.raises(error) as raised:
                    await batch.exec()
                if error is httpx.ReadError:
                    assert raised.value is transport_error
                elif error is UpstashError:
                    assert str(raised.value) == "WRONGTYPE"
                assert batch._command_stack == []
                batch.get("counter")
                assert await batch.exec() == ["1"]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_cancelled_batch_is_consumed(batch_method):
    started = asyncio.Event()
    requests = []

    async def handle(request):
        commands = json.loads(request.content)
        requests.append(commands)
        if len(requests) == 1:
            started.set()
            await asyncio.Event().wait()
        assert commands == [["GET", "counter"]]
        return httpx.Response(200, json=[{"result": "1"}])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        with patch("upstash_redis.http.httpx.AsyncClient", return_value=http):
            async with AsyncRedis(
                url="https://example.test",
                token="token",
                rest_retries=0,
                rest_encoding=None,
            ) as redis:
                batch = getattr(redis, batch_method)()
                batch.incr("counter")
                task = asyncio.create_task(batch.exec())
                await asyncio.wait_for(started.wait(), timeout=1)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert batch._command_stack == []
                batch.get("counter")
                assert await batch.exec() == ["1"]
    assert len(requests) == 2


def test_success_reset_and_context_exit(batch_method):
    requests = []

    def handle(request):
        commands = json.loads(request.content)
        requests.append(commands)
        if len(requests) == 1:
            return httpx.Response(
                200,
                json=[
                    {"result": "OK"},
                    {"result": ["ZmllbGQ=", "dmFsdWU="]},
                    {"result": None},
                    {"result": []},
                    {"result": ""},
                ],
            )
        assert commands == [["GET", "counter"]]
        return httpx.Response(200, json=[{"result": "MQ=="}])

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with patch("upstash_redis.http.httpx.Client", return_value=http):
            with Redis(url="https://example.test", token="token") as redis:
                batch = getattr(redis, batch_method)()
                batch.set("key", "value").hgetall("hash").get("missing").lrange(
                    "empty", 0, -1
                ).get("empty_string")
                assert batch.exec() == [True, {"field": "value"}, None, [], ""]
                assert batch._command_stack == []
                batch.incr("discarded").reset()
                with pytest.raises(ValueError, match="context failed"):
                    with batch:
                        batch.incr("discarded")
                        raise ValueError("context failed")
                batch.get("counter")
                assert batch.exec() == ["1"]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_async_success_reset_and_context_exit(batch_method):
    requests = []

    def handle(request):
        commands = json.loads(request.content)
        requests.append(commands)
        if len(requests) == 1:
            return httpx.Response(
                200,
                json=[
                    {"result": "OK"},
                    {"result": ["ZmllbGQ=", "dmFsdWU="]},
                    {"result": None},
                    {"result": []},
                    {"result": ""},
                ],
            )
        assert commands == [["GET", "counter"]]
        return httpx.Response(200, json=[{"result": "MQ=="}])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        with patch("upstash_redis.http.httpx.AsyncClient", return_value=http):
            async with AsyncRedis(url="https://example.test", token="token") as redis:
                batch = getattr(redis, batch_method)()
                batch.set("key", "value").hgetall("hash").get("missing").lrange(
                    "empty", 0, -1
                ).get("empty_string")
                assert await batch.exec() == [True, {"field": "value"}, None, [], ""]
                assert batch._command_stack == []
                batch.incr("discarded").reset()
                with pytest.raises(ValueError, match="context failed"):
                    async with batch:
                        batch.incr("discarded")
                        raise ValueError("context failed")
                batch.get("counter")
                assert await batch.exec() == ["1"]
    assert len(requests) == 2
