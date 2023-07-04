import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hincrbyfloat():
    hash_name = "myhash"
    field = "float_counter"

    # Set an initial value for the field
    redis.hset(hash_name, field, "2.5")

    # Increment the field value by a specific floating-point amount
    result = redis.hincrbyfloat(hash_name, field, 1.2)

    assert isinstance(result, float)

    assert result == pytest.approx(3.7)


def test_hincrbyfloat_without_formatting():
    redis.format_return = False

    hash_name = "myhash"
    field = "float_counter"

    # Set an initial value for the field
    redis.hset(hash_name, field, "2.5")

    # Increment the field value by a specific floating-point amount
    result = redis.hincrbyfloat(hash_name, field, 1.2)

    assert isinstance(result, str)

    redis.format_return = True
