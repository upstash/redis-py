from upstash_py.utils.exception import handle_georadius_write_exceptions
from pytest import raises


def test_with_invalid_any():
    with raises(Exception) as exception:
        handle_georadius_write_exceptions(count_any=True)

    assert str(exception.value) == "\"count_any\" can only be used together with \"count\"."


def test_with_additional_properties_and_store():
    with raises(Exception) as exception:
        handle_georadius_write_exceptions(with_hash=True, store_as="store_as")

    assert str(exception.value) == "Cannot use \"store_as\" or \"store_dist_as\" when requesting additional properties."


def test_with_additional_properties_and_storedist():
    with raises(Exception) as exception:
        handle_georadius_write_exceptions(with_distance=True, store_dist_as="store_dist_as")

    assert str(exception.value) == "Cannot use \"store_as\" or \"store_dist_as\" when requesting additional properties."


def test_with_count_and_any():
    assert handle_georadius_write_exceptions(count=1, count_any=True) is None


def test_without_additional_properties_and_store():
    assert handle_georadius_write_exceptions(store_as="store_as") is None


def test_without_additional_properties_and_storedist():
    assert handle_georadius_write_exceptions(store_dist_as="store_dist_as") is None
