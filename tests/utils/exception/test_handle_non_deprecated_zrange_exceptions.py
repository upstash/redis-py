from upstash_py.utils.exception import handle_non_deprecated_zrange_exceptions
from pytest import raises


def test_if_start_and_stop_are_not_strings() -> None:
    with raises(Exception) as exception:
        handle_non_deprecated_zrange_exceptions(
            range_method="BYLEX",
            start=0,
            stop=1,
            offset=None,
            count=None,
        )

    assert str(exception.value) == """"start" and "stop" must either start with '(' or '[' or be '+' or '-' when
the ranging method is "BYLEX"."""


def test_if_start_and_stop_are_incorrectly_formatted() -> None:
    with raises(Exception) as exception:
        handle_non_deprecated_zrange_exceptions(
            range_method="BYLEX",
            start="0",
            stop="1",
            offset=None,
            count=None,
        )

    assert str(exception.value) == """"start" and "stop" must either start with '(' or '[' or be '+' or '-' when
the ranging method is "BYLEX"."""


def test_with_invalid_offset_and_count() -> None:
    with raises(Exception) as exception:
        handle_non_deprecated_zrange_exceptions(
            range_method="BYLEX",
            start="(",
            stop="[",
            offset=None,
            count=1,
        )

    assert str(exception.value) == "Both \"offset\" and \"count\" must be specified."


def test_with_parenthesis_start_and_stop() -> None:
    assert handle_non_deprecated_zrange_exceptions(
        range_method="BYLEX",
        start="[",
        stop="(",
        offset=None,
        count=None,
    ) is None


def test_with_plus_and_minus_start_and_stop() -> None:
    assert handle_non_deprecated_zrange_exceptions(
        range_method=None,
        start="-",
        stop="+",
        offset=None,
        count=None,
    ) is None


def test_with_offset_and_count() -> None:
    assert handle_non_deprecated_zrange_exceptions(
        range_method=None,
        start=0,
        stop=1,
        offset=0,
        count=1,
    ) is None
