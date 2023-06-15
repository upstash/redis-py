from upstash_redis.utils.exception import handle_georadius_write_exceptions
from pytest import raises


def test_with_invalid_any():
    with raises(Exception) as exception:
        handle_georadius_write_exceptions(count_any=True)

    assert str(exception.value) == "\"count_any\" can only be used together with \"count\"."


def test_with_additional_properties_and_store():
    with raises(Exception) as exception:
        handle_georadius_write_exceptions(withhash=True, store="store_as")

    assert str(exception.value) == "Cannot use \"store\" or \"storedist\" when requesting additional properties."


def test_with_additional_properties_and_storedist():
    with raises(Exception) as exception:
        handle_georadius_write_exceptions(withdist=True, storedist="store_dist_as")

    assert str(exception.value) == "Cannot use \"store\" or \"storedist\" when requesting additional properties."


def test_with_count_and_count_any():
    handle_georadius_write_exceptions(count=1, count_any=True)


def test_without_additional_properties_and_store():
    handle_georadius_write_exceptions(store="store_as")


def test_without_additional_properties_and_storedist():
    handle_georadius_write_exceptions(storedist="store_dist_as")
