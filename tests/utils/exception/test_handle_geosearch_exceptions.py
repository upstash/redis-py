from upstash_redis.utils.exception import handle_geosearch_exceptions
from pytest import raises


def test_with_invalid_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member=None,
            fromlonlat_longitude=10,
            fromlonlat_latitude=None,
            byradius=None,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=False
        )

    assert str(exception.value) == "Both \"fromlonlat_longitude\" and \"fromlonlat_latitude\" must be specified."


def test_with_neither_member_nor_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member=None,
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=None,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=False
        )

    assert str(exception.value) == """Specify either the member's name with "member",
or the fromlonlat_longitude and fromlonlat_latitude with "fromlonlat_longitude" and "fromlonlat_latitude", but not both."""


def test_with_both_member_and_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=1.0,
            fromlonlat_latitude=2,
            byradius=None,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=True
        )

    assert str(exception.value) == """Specify either the member's name with "member",
or the fromlonlat_longitude and fromlonlat_latitude with "fromlonlat_longitude" and "fromlonlat_latitude", but not both."""


def test_with_invalid_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=None,
            bybox_width=2,
            bybox_height=None,
            count=None,
            count_any=False
        )

    assert str(exception.value) == "Both \"bybox_width\" and \"bybox_height\" must be specified."


def test_with_neither_radius_nor_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=None,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=False
        )

    assert str(exception.value) == """Specify either the byradius with "byradius",
or the bybox_width and bybox_height with "bybox_width" and "bybox_height", but not both."""


def test_with_both_radius_and_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=1,
            bybox_width=2,
            bybox_height=3,
            count=None,
            count_any=False
        )

    assert str(exception.value) == """Specify either the byradius with "byradius",
or the bybox_width and bybox_height with "bybox_width" and "bybox_height", but not both."""


def count_test_with_invalid_any() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            fromlonlat_longitude=None,
            fromlonlat_latitude=None,
            byradius=5,
            bybox_width=None,
            bybox_height=None,
            count=None,
            count_any=True
        )

    assert str(exception.value) == "\"count_any\" can only be used together with \"count\"."


def test_with_member_and_radius() -> None:
    handle_geosearch_exceptions(
        member="member",
        fromlonlat_longitude=None,
        fromlonlat_latitude=None,
        byradius=5,
        bybox_width=None,
        bybox_height=None,
        count=None,
        count_any=False
    )


def test_with_coordinates_and_width_and_height():
    handle_geosearch_exceptions(
        member=None,
        fromlonlat_longitude=1.0,
        fromlonlat_latitude=2,
        byradius=None,
        bybox_width=2.3,
        bybox_height=3.4,
        count=None,
        count_any=False
    )


def count_test_with_count_and_any():
    handle_geosearch_exceptions(
        member="member",
        fromlonlat_longitude=None,
        fromlonlat_latitude=None,
        byradius=5,
        bybox_width=None,
        bybox_height=None,
        count=10,
        count_any=True
    )
