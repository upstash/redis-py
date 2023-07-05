import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def load_scripts():
    redis.script_flush()
    yield
    redis.script_flush()


def test_script_flush():
    script1 = redis.script_load("return 1")
    script2 = redis.script_load("return 2")

    result = redis.script_exists(script1, script2)
    expected_result = [True, 1]
    assert result == expected_result

    redis.script_flush()

    result = redis.script_exists(script1, script2)

    expected_result = [False, False]
    assert result == expected_result
