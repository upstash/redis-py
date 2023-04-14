from upstash_py.utils.exception import (
    handle_geosearch_exceptions,
    handle_non_deprecated_zrange_exceptions,
    handle_zrangebylex_exceptions
)
from pytest import raises


def test_handle_geosearch_exceptions() -> None:
    """
    Test "handle_geosearch_exceptions" function.
    """

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

    # Test if neither "member" nor "longitude" and "latitude" are specified.
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

    # Test if both "member" and "longitude" and "latitude" are specified.
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

    # Test if neither "radius" nor "width" and "height" are specified.
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

    # Test if both "radius" and "width" and "height" are specified.
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
