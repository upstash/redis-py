import json

import httpx
import pytest

from upstash_redis import Redis
from upstash_redis.asyncio import Redis as AsyncRedis


@pytest.mark.parametrize("old_value", [None, "old-value", ""])
@pytest.mark.parametrize("pipeline", [False, True])
def test_sync_set_nx_get(old_value, pipeline):
    requests = []

    def handle(request):
        requests.append(json.loads(request.content))
        response = {"result": old_value}
        return httpx.Response(200, json=[response] if pipeline else response)

    with Redis(url="https://redis.invalid", token="test", rest_encoding=None) as client:
        client._http._client.close()
        client._http._client = httpx.Client(transport=httpx.MockTransport(handle))
        if pipeline:
            result = client.pipeline().set("unit-key", "new", nx=True, get=True).exec()
            assert result == [old_value]
        else:
            assert client.set("unit-key", "new", nx=True, get=True) == old_value
    command = ["SET", "unit-key", "new", "NX", "GET"]
    assert requests == [[command] if pipeline else command]


@pytest.mark.asyncio
@pytest.mark.parametrize("old_value", [None, "old-value", ""])
@pytest.mark.parametrize("pipeline", [False, True])
async def test_async_set_nx_get(old_value, pipeline):
    requests = []

    def handle(request):
        requests.append(json.loads(request.content))
        response = {"result": old_value}
        return httpx.Response(200, json=[response] if pipeline else response)

    async with AsyncRedis(
        url="https://redis.invalid", token="test", rest_encoding=None
    ) as client:
        await client._http._client.aclose()
        client._http._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        if pipeline:
            result = (
                await client.pipeline().set("unit-key", "new", nx=True, get=True).exec()
            )
            assert result == [old_value]
        else:
            assert await client.set("unit-key", "new", nx=True, get=True) == old_value
    command = ["SET", "unit-key", "new", "NX", "GET"]
    assert requests == [[command] if pipeline else command]


@pytest.mark.parametrize("pipeline", [False, True])
def test_sync_set_nx_xx_remains_invalid(pipeline):
    with Redis(url="https://redis.invalid", token="test") as client:
        target = client.pipeline() if pipeline else client
        with pytest.raises(Exception, match='"nx" and "xx" are mutually exclusive'):
            target.set("unit-key", "new", nx=True, xx=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("pipeline", [False, True])
async def test_async_set_nx_xx_remains_invalid(pipeline):
    async with AsyncRedis(url="https://redis.invalid", token="test") as client:
        with pytest.raises(Exception, match='"nx" and "xx" are mutually exclusive'):
            if pipeline:
                client.pipeline().set("unit-key", "new", nx=True, xx=True)
            else:
                await client.set("unit-key", "new", nx=True, xx=True)
