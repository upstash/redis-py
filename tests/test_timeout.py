from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread

import httpx
import pytest

from upstash_redis import Redis
from upstash_redis.asyncio import Redis as AsyncRedis


@pytest.mark.parametrize("timeout", ["default", None, 2.5])
@pytest.mark.parametrize("from_env", [False, True])
@pytest.mark.parametrize("operation", ["command", "pipeline", "transaction"])
def test_sync_timeout_reaches_requests(monkeypatch, timeout, from_env, operation):
    monkeypatch.setenv("UPSTASH_REDIS_REST_URL", "https://redis.example")
    monkeypatch.setenv("UPSTASH_REDIS_REST_TOKEN", "token")
    requests = []

    def handle_request(self, request):
        requests.append(request)
        result = {"result": "PONG"}
        return httpx.Response(200, json=result if operation == "command" else [result])

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", handle_request)
    options = {"rest_encoding": None}
    if timeout == "default":
        timeout = None
    else:
        options["rest_timeout"] = timeout
    client = (
        Redis.from_env(**options)
        if from_env
        else Redis("https://redis.example", "token", **options)
    )
    with client:
        if operation == "command":
            assert client.ping() == "PONG"
        else:
            pipeline = client.pipeline() if operation == "pipeline" else client.multi()
            assert pipeline.ping().exec() == ["PONG"]
    assert len(requests) == 1
    assert requests[0].extensions["timeout"] == {
        "connect": timeout,
        "read": timeout,
        "write": timeout,
        "pool": timeout,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", ["default", None, 2.5])
@pytest.mark.parametrize("from_env", [False, True])
@pytest.mark.parametrize("operation", ["command", "pipeline", "transaction"])
async def test_async_timeout_reaches_requests(
    monkeypatch, timeout, from_env, operation
):
    monkeypatch.setenv("UPSTASH_REDIS_REST_URL", "https://redis.example")
    monkeypatch.setenv("UPSTASH_REDIS_REST_TOKEN", "token")
    requests = []

    async def handle_request(self, request):
        requests.append(request)
        result = {"result": "PONG"}
        return httpx.Response(200, json=result if operation == "command" else [result])

    monkeypatch.setattr(
        httpx.AsyncHTTPTransport, "handle_async_request", handle_request
    )
    options = {"rest_encoding": None}
    if timeout == "default":
        timeout = None
    else:
        options["rest_timeout"] = timeout
    client = (
        AsyncRedis.from_env(**options)
        if from_env
        else AsyncRedis("https://redis.example", "token", **options)
    )
    async with client:
        if operation == "command":
            assert await client.ping() == "PONG"
        else:
            pipeline = client.pipeline() if operation == "pipeline" else client.multi()
            assert await pipeline.ping().exec() == ["PONG"]
    assert len(requests) == 1
    assert requests[0].extensions["timeout"] == {
        "connect": timeout,
        "read": timeout,
        "write": timeout,
        "pool": timeout,
    }


@pytest.fixture
def stalled_server(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    release = Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            release.wait(5)
            self.close_connection = True

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port)
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join()


def test_sync_read_timeout(stalled_server):
    with Redis(stalled_server, "token", rest_timeout=0.05, rest_retries=0) as client:
        with pytest.raises(httpx.ReadTimeout):
            client.ping()


@pytest.mark.asyncio
async def test_async_read_timeout(stalled_server):
    async with AsyncRedis(
        stalled_server, "token", rest_timeout=0.05, rest_retries=0
    ) as client:
        with pytest.raises(httpx.ReadTimeout):
            await client.ping()


@pytest.mark.parametrize("succeed", [False, True])
def test_sync_timeout_retries(monkeypatch, succeed):
    requests = []

    def handle_request(self, request):
        requests.append(request)
        if len(requests) < 3 or not succeed:
            raise httpx.ReadTimeout("read timed out", request=request)
        return httpx.Response(200, json={"result": "PONG"})

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", handle_request)
    with Redis(
        "https://redis.example",
        "token",
        rest_timeout=1,
        rest_retries=2,
        rest_retry_interval=0,
        rest_encoding=None,
    ) as client:
        if succeed:
            assert client.ping() == "PONG"
        else:
            with pytest.raises(httpx.ReadTimeout):
                client.ping()
    assert len(requests) == 3
    assert all(request.extensions["timeout"]["read"] == 1 for request in requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("succeed", [False, True])
async def test_async_timeout_retries(monkeypatch, succeed):
    requests = []

    async def handle_request(self, request):
        requests.append(request)
        if len(requests) < 3 or not succeed:
            raise httpx.ReadTimeout("read timed out", request=request)
        return httpx.Response(200, json={"result": "PONG"})

    monkeypatch.setattr(
        httpx.AsyncHTTPTransport, "handle_async_request", handle_request
    )
    async with AsyncRedis(
        "https://redis.example",
        "token",
        rest_timeout=1,
        rest_retries=2,
        rest_retry_interval=0,
        rest_encoding=None,
    ) as client:
        if succeed:
            assert await client.ping() == "PONG"
        else:
            with pytest.raises(httpx.ReadTimeout):
                await client.ping()
    assert len(requests) == 3
    assert all(request.extensions["timeout"]["read"] == 1 for request in requests)
