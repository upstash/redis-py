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
        handle_georadius_write_exceptions(count_any=True)

    assert str(exception.value) == '"count_any" can only be used together with "count".'


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


def test_handle_georadius_exception_with_count_and_count_any():
    handle_georadius_write_exceptions(count=1, count_any=True)


def test_handle_georadius_exception_without_additional_properties_and_store():
    handle_georadius_write_exceptions(store="store_as")


def test_handle_georadius_exception_without_additional_properties_and_storedist():
    handle_georadius_write_exceptions(storedist="store_dist_as")


def test_handle_geosearch_with_invalid_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member=None,
            fromlonlat_longitude=10,
            fromlonlat_latitude=None,
            byradius=None,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=False,
        )

    assert (
        str(exception.value)
        == 'Both "fromlonlat_longitude" and "fromlonlat_latitude" must be specified.'
    )


def test_handle_geosearch_with_neither_member_nor_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member=None,
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=None,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=False,
        )

    assert (
        str(exception.value)
        == """Specify either the member's name with "member",
or the fromlonlat_longitude and fromlonlat_latitude with "fromlonlat_longitude" and "fromlonlat_latitude", but not both."""
    )


def test_handle_geosearch_with_both_member_and_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=1.0,
            fromlonlat_latitude=2,
            byradius=None,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=True,
        )

    assert (
        str(exception.value)
        == """Specify either the member's name with "member",
or the fromlonlat_longitude and fromlonlat_latitude with "fromlonlat_longitude" and "fromlonlat_latitude", but not both."""
    )


def test_handle_geosearch_with_invalid_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=None,
            bybox_width=2,
            bybox_height=None,
            count=None,
            count_any=False,
        )

    assert (
        str(exception.value)
        == 'Both "bybox_width" and "bybox_height" must be specified.'
    )


def test_handle_geosearch_with_neither_radius_nor_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=None,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=False,
        )

    assert (
        str(exception.value)
        == """Specify either the byradius with "byradius",
or the bybox_width and bybox_height with "bybox_width" and "bybox_height", but not both."""
    )


def test_handle_geosearch_with_both_radius_and_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=1,
            bybox_width=2,
            bybox_height=3,
            count=None,
            count_any=False,
        )

    assert (
        str(exception.value)
        == """Specify either the byradius with "byradius",
or the bybox_width and bybox_height with "bybox_width" and "bybox_height", but not both."""
    )


def test_handle_geosearch_count_with_invalid_any() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=5,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=True,
        )

    assert str(exception.value) == '"count_any" can only be used together with "count".'


def test_handle_geosearch_with_member_and_radius() -> None:
    handle_geosearch_exceptions(
        member="member",
        fromlonlat_longitude=None,
        fromlonlat_latitude=None,
        byradius=5,
        bybox_width=None,
        bybox_height=None,
        count=None,
        count_any=False,
    )


def test_handle_geosearch_with_coordinates_and_width_and_height():
    handle_geosearch_exceptions(
        member=None,
        fromlonlat_longitude=1.0,
        fromlonlat_latitude=2,
        byradius=None,
        bybox_width=2.3,
        bybox_height=3.4,
        count=None,
        count_any=False,
    )


def test_handle_geosearch_count_with_count_and_any():
    handle_geosearch_exceptions(
        member="member",
        fromlonlat_longitude=None,
        fromlonlat_latitude=None,
        byradius=5,
        bybox_width=None,
        bybox_height=None,
        count=10,
        count_any=True,
    )


def test_handle_non_deprecated_zrange_if_start_and_stop_are_not_strings() -> None:
    with raises(Exception) as exception:
        handle_non_deprecated_zrange_exceptions(
            range_method="BYLEX",
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
            range_method="BYLEX",
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
            range_method="BYLEX",
            start="(",
            stop="[",
            offset=None,
            count=1,
        )

    assert str(exception.value) == 'Both "offset" and "count" must be specified.'


def test_handle_non_deprecated_zrange_with_parenthesis_start_and_stop() -> None:
    handle_non_deprecated_zrange_exceptions(
        range_method="BYLEX",
        start="[",
        stop="(",
        offset=None,
        count=None,
    )


def test_handle_non_deprecated_zrange_with_plus_and_minus_start_and_stop() -> None:
    handle_non_deprecated_zrange_exceptions(
        range_method=None,
        start="-",
        stop="+",
        offset=None,
        count=None,
    )


def test_handle_non_deprecated_zrange_with_offset_and_count() -> None:
    handle_non_deprecated_zrange_exceptions(
        range_method=None,
        start=0,
        stop=1,
        offset=0,
        count=1,
    )


def test_handle_zrangebylex_if_min_and_max_score_are_incorrectly_formatted() -> None:
    with raises(Exception) as exception:
        handle_zrangebylex_exceptions(
            min_score="0",
            max_score="1",
            offset=None,
            count=None,
        )

    assert (
        str(exception.value)
        == """"min_score" and "max_score" must either start with '(' or '[' or be '+' or '-'."""
    )


def test_handle_zrangebylex_with_invalid_offset_and_count() -> None:
    with raises(Exception) as exception:
        handle_zrangebylex_exceptions(
            min_score="(",
            max_score="[",
            offset=None,
            count=1,
        )

    assert str(exception.value) == 'Both "offset" and "count" must be specified.'


def test_handle_zrangebylex_with_parenthesis_min_score_and_max_score() -> None:
    handle_zrangebylex_exceptions(
        min_score="[",
        max_score="(",
        offset=None,
        count=None,
    )


def test_handle_zrangebylex_with_plus_and_minus_min_and_max_score() -> None:
    handle_zrangebylex_exceptions(
        min_score="-",
        max_score="+",
        offset=None,
        count=None,
    )


def test_handle_zrangebylex_with_offset_and_count() -> None:
    handle_zrangebylex_exceptions(
        min_score="(",
        max_score="[",
        offset=0,
        count=1,
    )
