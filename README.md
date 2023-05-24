# Upstash Rate Limit - python edition

upstash-redis is a connectionless, HTTP-based Redis client for python, designed to be used in serverless and serverful environments such as:
- AWS Lambda
- Google Cloud Functions
- and other environments where HTTP is preferred over TCP.

Inspired by other Redis clients like [@upstash/redis](https://github.com/upstash/upstash-redis) and [redis-py](https://github.com/redis/redis-py),
the goal of this SDK is to provide a simple, dx-enchanted way to use Redis in HTTP-based environments and not only.

It is `async-based` and supports `typing` features.

The SDK is currently compatible with python 3.10 and above.

<!-- toc -->

- [Quick Start](#quick-start)
  - [Install](#install)
    - [PyPi](#PyPi)
  - [Usage](#usage)
  - [Telemetry](#telemetry)
- [Encoding](#encoding)
- [Retry mechanism](#retry-mechanism)
- [Formatting returns](#formatting-returns)
- [Contributing](#contributing)
  - [Preparing the environment](#preparing-the-environment)
  - [Config](#config)
  - [Adding new commands](#adding-new-commands)
  - [Testing](#testing)
  - [Releasing](#releasing)
- [Important](#important)

<!-- tocstop -->

# Quick Start

## Install

### PyPi
```bash
pip install upstash-redis
```

If you are using a packaging and dependency management tool like [Poetry](https://python-poetry.org), you might want to check
the respective docs in regard to adding a dependency. For example, in a Poetry-managed virtual environment, you can use
```bash
poetry add upstash-redis
```


## Usage
To be able to use upstash-redis, you need to create a database on [Upstash](https://console.upstash.com/)
and grab `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN` from the console.

```python
from upstash_redis.client import Redis

redis = Redis(url="UPSTASH_REDIS_REST_URL", token="UPSTASH_REDIS_REST_TOKEN")
```

Or, if you want to automatically load the credentials from the environment:

```python
from upstash_redis.client import Redis

redis = Redis.from_env()
```

upstash-redis uses [aiohttp](https://docs.aiohttp.org/en/stable/) for handling the HTTP calls.
If you are interested in the low-level aspects of how requests are performed, check the [aiohttp docs](https://docs.aiohttp.org/en/stable/http_request_lifecycle.html).
If not, all you need to know is that the `Redis` class acts both as a command grouper and as an [async context manager](https://superfastpython.com/asynchronous-context-manager/).

If you are in a serverless environment that allows it, it's also recommended to initialise the client outside the request handler
to be reused while your function is still hot.

Running commands might look like this:

```python
from upstash_redis.client import Redis
from asyncio import run

redis = Redis.from_env()


async def main():
    async with redis:
        await redis.set("a", "b")
        print(await redis.get("a"))

run(main())

```


# Telemetry
The SDK can collect the following anonymous telemetry:
  - the runtime (ex: `python@v.3.10.0`)
  - the SDK or SDKs you're using (ex: `upstash-redisy@development, upstash-ratelimit@v.0.1.0`)
  - the platform you're running on (ex: `AWS-lambda`)

If you want to opt out, simply set `allow_telemetry` to `False` in the `Redis` constructor or the `from_env` class method.


# Encoding
Although Redis can store invalid JSON data, there might be problems with the deserialization.
To avoid this, the Upstash REST proxy is capable of encoding the data as base64 on the server and then sending it to the client to be
decoded. 

For very large data, this can add a few milliseconds in latency. So, if you're sure that your data is valid JSON, you can set
`rest_encoding` to `False`.


# Retry mechanism
upstash-redis has a fallback mechanism in case of network or API issues. By default, if a request fails it'll retry once, 3 seconds 
after the error. If you want to customize that, set `rest_retries` and `rest_retry_interval` (in seconds).


# Formatting returns
The SDK relies on the Upstash REST proxy, which returns the `RESP2` responses of the given commands.
By default, we apply formatting to some of them to provide a better developer experience.
If you want the commands to output the raw return, set `format_return` to `False`.

You can also opt out of individual commands:

```python
redis.format_return = False

print(await redis.copy(source="source_string", destination="destination_string"))

redis.format_return = True
```

One particular formatting feature is the `return_cursor` of the `SCAN`-related commands.
If it is `False`, only the list of results will be returned.


# Contributing

## Preparing the environment
This project uses [Poetry](https://python-poetry.org) for packaging and dependency management. 

See [this](https://python-poetry.org/docs/basic-usage/#using-your-virtual-environment) for a detailed explanation on how
to work with the virtual environment.

You will also need a database on [Upstash](https://console.upstash.com/).


## Config
Most of the config variables and defaults are stored in [config.py](./upstash_redis/config.py).

The defaults for telemetry are set directly in the [execute](./upstash_redis/http/execute.py) function.


# Adding new commands
Commands should try to have a developer-friendly signature and each deviation from the Redis API must be documented with
docstrings.

To increase the DX even more, the commands should try to raise exceptions whenever parameters logics is misunderstood
and can't be deducted simply from the types. Example: `bitcount` in [client.py](./upstash_redis/client.py)

The goal is to also isolate the complex formatting functions from the commands themselves.

For chained (ex: [BITFIELD](https://redis.io/commands/bitfield/)) or semantically-grouped (ex: `SCRIPT {command}`) commands, a
separate class should be created.


# Testing
upstash-redis aims to have a simple and reliable testing strategy, defining cases based on both the signature of the commands
and their desired behaviour. Why is this important? Because testing almost any possible command form when the parameters logic 
doesn't change is more like testing the REST API, not only the client.

Tests should use named parameters wherever it may improve readability and should use the [execute_on_http](./tests/execute_on_http.py)
function directly instead of the SDK itself whenever an extra command is needed to ensure atomicity.

There is also the [prepare_database](./tests/prepare_database.py) script whose goal is to flush and then seed the selected testing
database with the required keys. In this way, no random values have to be used.

To run all the tests, make sure you are in the tests folder and have the poetry virtual environment activated with all the necessary 
dependencies and set the `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN` environment variables.

Prepare the database (running this will empty your destination database):

```bash
poetry run python prepare_database.py
```

Run the tests:

```bash
poetry run pytest
```


# Releasing
To create a new release, first use Poetry's [version](https://python-poetry.org/docs/cli/#version) command.

You will then need to connect your PyPi API token to Poetry. 
A simple tutorial showcasing how to do it was posted by Tony Tran
[on DigitalOcean](https://www.digitalocean.com/community/tutorials/how-to-publish-python-packages-to-pypi-using-poetry-on-ubuntu-22-04)

From there, use `poetry publish --build`.


# Important!
This branch's role is to provide a python 3.10-compatible version to allow the use
in platforms such as AWS Lambda.

For that to be possible, some typing features like the "Self" type and the Generic 
TypedDict were removed.

Currently, this is the only branch from which the SDK is released, and it holds the
version used by the rate-limit SDK too.