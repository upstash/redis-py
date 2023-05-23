from upstash_redis.exception import UpstashException
from upstash_redis.http.decode import decode
from upstash_redis.schema.http import RESTResult, RESTResponse, RESTEncoding
from upstash_redis.schema.telemetry import TelemetryData
from asyncio import sleep
from aiohttp import ClientSession
from json import dumps
from platform import python_version


async def execute(
    session: ClientSession,
    url: str,
    token: str,
    encoding: RESTEncoding,
    retries: int,
    retry_interval: int,
    command: list,
    allow_telemetry: bool,
    telemetry_data: TelemetryData | None = None
) -> RESTResult:
    """
    Execute the given command over the REST API.
    """

    # Serialize the command; more specifically, write string-incompatible types as JSON strings.
    command = [
        element if isinstance(element, str | int | float)

        else dumps(element)

        for element in command
    ]

    for i in range(retries + 1):
        try:
            headers: dict[str, str] = {"Authorization": f'Bearer {token}'}

            if allow_telemetry:
                if telemetry_data:
                    # Avoid the [] syntax to prevent KeyError from being raised.
                    if telemetry_data.get("runtime"):
                        headers["Upstash-Telemetry-Runtime"] = telemetry_data["runtime"]
                    else:
                        headers["Upstash-Telemetry-Runtime"] = f'python@v{python_version()}'

                    if telemetry_data.get("sdk"):
                        headers["Upstash-Telemetry-Sdk"] = telemetry_data["sdk"]
                    else:
                        headers["Upstash-Telemetry-Sdk"] = "upstash_redis@development"

                    if telemetry_data.get("platform"):
                        headers["Upstash-Telemetry-Platform"] = telemetry_data["platform"]

            if encoding:
                headers["Upstash-Encoding"] = encoding

            async with session.post(url, headers=headers, json=command) as response:
                body: RESTResponse = await response.json()

                # Avoid the [] syntax to prevent KeyError from being raised.
                if body.get("error"):
                    raise UpstashException(body.get("error"))

                return decode(raw=body["result"], encoding=encoding) if encoding else body["result"]
            break
        except Exception as exception:
            if i == retries:
                # If we exhausted all the retries, raise the exception.
                raise exception
            else:
                await sleep(retry_interval)
