from pytest import raises

from upstash_redis.utils import (
    handle_georadius_write_exceptions,
    handle_geosearch_exceptions,
    handle_non_deprecated_zrange_exceptions,
    handle_zrangebylex_exceptions,
    number_are_not_none,
)


def test_number_are_not_none_with_one_not_none_parameter() -> None:
    assert number_are_not_none(2, None, None, number=1)


def test_number_are_not_none_with_all_not_none_parameters() -> None:
    assert number_are_not_none(1, 2, 3, number=3)


def test_number_are_not_none_with_all_none_parameters() -> None:
    assert number_are_not_none(None, None, None, number=0)


def test_number_are_not_none_with_more_than_number_not_none_parameters() -> None:
    assert not number_are_not_none(1, 2, 3, number=1)


def test_number_are_not_none_with_all_none_parameters_and_positive_number() -> None:
    assert not number_are_not_none(None, None, None, number=1)


def test_handle_georadius_exception_with_invalid_any():
    with raises(Exception) as exception:
        handle_georadius_write_exceptions(any=True)

    assert str(exception.value) == '"any" can only be used together with "count".'


def test_handle_georadius_exception_with_additional_properties_and_store():
    with raises(Exception) as exception:
        handle_georadius_write_exceptions(withhash=True, store="store_as")

    assert (
        str(exception.value)
        == 'Cannot use "store" or "storedist" when requesting additional properties.'
    )


def test_handle_georadius_exception_with_additional_properties_and_storedist():
    with raises(Exception) as exception:
        handle_georadius_write_exceptions(withdist=True, storedist="store_dist_as")

    assert (
        str(exception.value)
        == 'Cannot use "store" or "storedist" when requesting additional properties.'
    )


def test_handle_georadius_exception_with_count_and_any():
    handle_georadius_write_exceptions(count=1, any=True)


def test_handle_georadius_exception_without_additional_properties_and_store():
    handle_georadius_write_exceptions(store="store_as")


def test_handle_georadius_exception_without_additional_properties_and_storedist():
    handle_georadius_write_exceptions(storedist="store_dist_as")


def test_handle_geosearch_with_invalid_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member=None,
            longitude=10,
            latitude=None,
            radius=None,
            width=None,
            height=None,
            count=None,
            any=False,
        )

    assert str(exception.value) == 'Both "longitude" and "latitude" must be specified.'


def test_handle_geosearch_with_neither_member_nor_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member=None,
            longitude=None,
            latitude=None,
            radius=None,
            width=None,
            height=None,
            count=None,
            any=False,
        )

    assert (
        str(exception.value)
        == """Specify either the member's name with "member", or the "longitude" and "latitude", but not both."""
    )


def test_handle_geosearch_with_both_member_and_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=1.0,
            latitude=2,
            radius=None,
            width=None,
            height=None,
            count=None,
            any=True,
        )

    assert (
        str(exception.value)
        == """Specify either the member's name with "member", or the "longitude" and "latitude", but not both."""
    )


def test_handle_geosearch_with_invalid_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=None,
            latitude=None,
            radius=None,
            width=2,
            height=None,
            count=None,
            any=False,
        )

    assert str(exception.value) == 'Both "width" and "height" must be specified.'


def test_handle_geosearch_with_neither_radius_nor_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=None,
            latitude=None,
            radius=None,
            width=None,
            height=None,
            count=None,
            any=False,
        )

    assert (
        str(exception.value)
        == """Specify either the "radius", or the "width" and "height", but not both."""
    )


def test_handle_geosearch_with_both_radius_and_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=None,
            latitude=None,
            radius=1,
            width=2,
            height=3,
            count=None,
            any=False,
        )

    assert (
        str(exception.value)
        == """Specify either the "radius", or the "width" and "height", but not both."""
    )


def test_handle_geosearch_count_with_invalid_any() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=None,
            latitude=None,
            radius=5,
            width=None,
            height=None,
            count=None,
            any=True,
        )

    assert str(exception.value) == '"any" can only be used together with "count".'


def test_handle_geosearch_with_member_and_radius() -> None:
    handle_geosearch_exceptions(
        member="member",
        longitude=None,
        latitude=None,
        radius=5,
        width=None,
        height=None,
        count=None,
        any=False,
    )


def test_handle_geosearch_with_coordinates_and_width_and_height():
    handle_geosearch_exceptions(
        member=None,
        longitude=1.0,
        latitude=2,
        radius=None,
        width=2.3,
        height=3.4,
        count=None,
        any=False,
    )


def test_handle_geosearch_count_with_count_and_any():
    handle_geosearch_exceptions(
        member="member",
        longitude=None,
        latitude=None,
        radius=5,
        width=None,
        height=None,
        count=10,
        any=True,
    )


def test_handle_non_deprecated_zrange_if_start_and_stop_are_not_strings() -> None:
    with raises(Exception) as exception:
        handle_non_deprecated_zrange_exceptions(
            sortby="BYLEX",
            start=0,
            stop=1,
            offset=None,
            count=None,
        )

    assert (
        str(exception.value)
        == """"start" and "stop" must either start with '(' or '[' or be '+' or '-' when
the ranging method is "BYLEX"."""
    )


def test_handle_non_deprecated_zrange_if_start_and_stop_are_incorrectly_formatted() -> (
    None
):
    with raises(Exception) as exception:
        handle_non_deprecated_zrange_exceptions(
            sortby="BYLEX",
            start="0",
            stop="1",
            offset=None,
            count=None,
        )

    assert (
        str(exception.value)
        == """"start" and "stop" must either start with '(' or '[' or be '+' or '-' when
the ranging method is "BYLEX"."""
    )


def test_handle_non_deprecated_zrange_with_invalid_offset_and_count() -> None:
    with raises(Exception) as exception:
        handle_non_deprecated_zrange_exceptions(
            sortby="BYLEX",
            start="(",
            stop="[",
            offset=None,
            count=1,
        )

    assert str(exception.value) == 'Both "offset" and "count" must be specified.'


def test_handle_non_deprecated_zrange_with_parenthesis_start_and_stop() -> None:
    handle_non_deprecated_zrange_exceptions(
        sortby="BYLEX",
        start="[",
        stop="(",
        offset=None,
        count=None,
    )


def test_handle_non_deprecated_zrange_with_plus_and_minus_start_and_stop() -> None:
    handle_non_deprecated_zrange_exceptions(
        sortby=None,
        start="-",
        stop="+",
        offset=None,
        count=None,
    )


def test_handle_non_deprecated_zrange_with_offset_and_count() -> None:
    handle_non_deprecated_zrange_exceptions(
        sortby=None,
        start=0,
        stop=1,
        offset=0,
        count=1,
    )


def test_handle_zrangebylex_if_min_and_max_are_incorrectly_formatted() -> None:
    with raises(Exception) as exception:
        handle_zrangebylex_exceptions(
            min="0",
            max="1",
            offset=None,
            count=None,
        )

    assert (
        str(exception.value)
        == """"min" and "max" must either start with '(' or '[' or be '+' or '-'."""
    )


def test_handle_zrangebylex_with_invalid_offset_and_count() -> None:
    with raises(Exception) as exception:
        handle_zrangebylex_exceptions(
            min="(",
            max="[",
            offset=None,
            count=1,
        )

    assert str(exception.value) == 'Both "offset" and "count" must be specified.'


def test_handle_zrangebylex_with_parenthesis_min_and_max() -> None:
    handle_zrangebylex_exceptions(
        min="[",
        max="(",
        offset=None,
        count=None,
    )


def test_handle_zrangebylex_with_plus_and_minus_min_and_max() -> None:
    handle_zrangebylex_exceptions(
        min="-",
        max="+",
        offset=None,
        count=None,
    )


def test_handle_zrangebylex_with_offset_and_count() -> None:
    handle_zrangebylex_exceptions(
        min="(",
        max="[",
        offset=0,
        count=1,
    )
