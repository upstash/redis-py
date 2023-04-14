from upstash_py.utils.exception import (
    handle_geosearch_exceptions,
    handle_non_deprecated_zrange_exceptions,
    handle_zrangebylex_exceptions
)
from pytest import raises


def test_handle_geosearch_exceptions() -> None:
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

    # Test if no exception is raised when only "member" and "radius" are specified.
    assert handle_geosearch_exceptions(
        member="member",
        longitude=None,
        latitude=None,
        radius=5,
        width=None,
        height=None,
        count=None,
        count_any=False
    ) is None

    """
    Test if no exception is raised when "longitude" and "latitude", respectively "width" and "height" are specified,
    "count" is given and "count_any" is set to True.
    """
    assert handle_geosearch_exceptions(
        member=None,
        longitude=1.0,
        latitude=2,
        radius=None,
        width=2.3,
        height=3.4,
        count=2,
        count_any=True
    ) is None


def test_handle_non_deprecated_zrange_exceptions() -> None:
    # Test if "start" and "stop" are not strings.
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

    # Test if "start" and "stop" are incorrectly-formatted strings.
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

    with raises(Exception) as exception:
        handle_non_deprecated_zrange_exceptions(
            range_method="BYLEX",
            start="(",
            stop="[",
            offset=None,
            count=1,
        )

    assert str(exception.value) == "Both \"offset\" and \"count\" must be specified."

    # Test if no exception is raised when "start" and "stop" start with '(' or '['.
    assert handle_non_deprecated_zrange_exceptions(
        range_method="BYLEX",
        start="[",
        stop="(",
        offset=None,
        count=None,
    ) is None

    # Test if no exception is raised when "start" and "stop" start are '+' or '-'.
    assert handle_non_deprecated_zrange_exceptions(
        range_method="BYLEX",
        start="+",
        stop="-",
        offset=None,
        count=None,
    ) is None

    # Test if no exception is raised when both "offset" and "count" are specified and range_method is not "BYLEX".
    assert handle_non_deprecated_zrange_exceptions(
        range_method=None,
        start=1,
        stop=2,
        offset=0,
        count=1,
    ) is None


def test_handle_zrangebylex_exceptions() -> None:
    # Test if "min_score" and "max_score" are incorrectly-formatted strings.
    with raises(Exception) as exception:
        handle_zrangebylex_exceptions(
            min_score="0",
            max_score="1",
            offset=None,
            count=None,
        )

    assert str(exception.value) == """"min_score" and "max_score" must either start with '(' or '[' or be '+' or '-'."""

    with raises(Exception) as exception:
        handle_zrangebylex_exceptions(
            min_score="(",
            max_score="[",
            offset=None,
            count=1,
        )

    assert str(exception.value) == "Both \"offset\" and \"count\" must be specified."

    # Test if no exception is raised when "min_score" and "max_score" start with '(' or '['.
    assert handle_zrangebylex_exceptions(
        min_score="[",
        max_score="(",
        offset=None,
        count=None,
    ) is None

    # Test if no exception is raised when "min_score" and "max_score" start are '+' or '-'.
    assert handle_zrangebylex_exceptions(
        min_score="+",
        max_score="-",
        offset=None,
        count=None,
    ) is None

    # Test if no exception is raised when both "offset" and "count" are specified.
    assert handle_zrangebylex_exceptions(
        min_score="(",
        max_score="[",
        offset=0,
        count=1,
    ) is None
