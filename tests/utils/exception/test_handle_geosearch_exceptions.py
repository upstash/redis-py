from upstash_redis.utils.exception import handle_geosearch_exceptions
from pytest import raises


def test_with_invalid_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member=None,
            longitude=10,
            latitude=None,
            radius=None,
            width=None,
            height=None,
            count=None,
            count_any=False
        )

    assert str(exception.value) == "Both \"longitude\" and \"latitude\" must be specified."


def test_with_neither_member_nor_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member=None,
            longitude=None,
            latitude=None,
            radius=None,
            width=None,
            height=None,
            count=None,
            count_any=False
        )

    assert str(exception.value) == """Specify either the member's name with "member",
or the longitude and latitude with "longitude" and "latitude", but not both."""


def test_with_both_member_and_longitude_and_latitude() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=1.0,
            latitude=2,
            radius=None,
            width=None,
            height=None,
            count=None,
            count_any=True
        )

    assert str(exception.value) == """Specify either the member's name with "member",
or the longitude and latitude with "longitude" and "latitude", but not both."""


def test_with_invalid_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=None,
            latitude=None,
            radius=None,
            width=2,
            height=None,
            count=None,
            count_any=False
        )

    assert str(exception.value) == "Both \"width\" and \"height\" must be specified."


def test_with_neither_radius_nor_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=None,
            latitude=None,
            radius=None,
            width=None,
            height=None,
            count=None,
            count_any=False
        )

    assert str(exception.value) == """Specify either the radius with "radius",
or the width and height with "width" and "height", but not both."""


def test_with_both_radius_and_width_and_height() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=None,
            latitude=None,
            radius=1,
            width=2,
            height=3,
            count=None,
            count_any=False
        )

    assert str(exception.value) == """Specify either the radius with "radius",
or the width and height with "width" and "height", but not both."""


def test_with_invalid_any() -> None:
    with raises(Exception) as exception:
        handle_geosearch_exceptions(
            member="member",
            longitude=None,
            latitude=None,
            radius=5,
            width=None,
            height=None,
            count=None,
            count_any=True
        )

    assert str(exception.value) == "\"count_any\" can only be used together with \"count\"."


def test_with_member_and_radius() -> None:
    handle_geosearch_exceptions(
        member="member",
        longitude=None,
        latitude=None,
        radius=5,
        width=None,
        height=None,
        count=None,
        count_any=False
    )


def test_with_coordinates_and_width_and_height():
    handle_geosearch_exceptions(
        member=None,
        longitude=1.0,
        latitude=2,
        radius=None,
        width=2.3,
        height=3.4,
        count=None,
        count_any=False
    )


def test_with_count_and_any():
    handle_geosearch_exceptions(
        member="member",
        longitude=None,
        latitude=None,
        radius=5,
        width=None,
        height=None,
        count=10,
        count_any=True
    )
