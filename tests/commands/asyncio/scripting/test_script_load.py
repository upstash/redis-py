import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_scripts():
    redis.script_flush()
    yield
    redis.script_flush()


def test_script_load():
    script1 = "return 1"
    script2 = "return 2"

    script1_sha = redis.script_load(script1)
    script2_sha = redis.script_load(script2)

    res = redis.evalsha(script1_sha)
    assert res == 1

    result = redis.script_exists(script1_sha, script2_sha)

    expected_result = [True, True]
    assert result == expected_result
