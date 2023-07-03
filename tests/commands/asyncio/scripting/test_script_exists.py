import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_scripts():
    redis.script_flush()
    yield
    redis.script_flush()


def test_script_exists():
    sha1 = redis.script_load("return 1")
    sha2 = redis.script_load("return 2")

    result = redis.script_exists(sha1, sha2)

    expected_result = [True, 1]
    assert result == expected_result

    result = redis.script_exists("non_existing_sha")

    expected_result = [False]
    assert result == expected_result
