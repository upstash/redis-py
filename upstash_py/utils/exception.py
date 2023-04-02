from upstash_py.schema.commands.parameters import FloatMinMax
from upstash_py.utils.comparison import all_are_specified, one_is_specified
from typing import Literal


def handle_geosearch_exceptions(
    member: str | None,
    longitude: float | None,
    latitude: float | None,
    radius: float | None,
    width: float | None,
    height: float | None,
    count: int | None,
    count_any: bool
) -> None:
    """
    Handle exceptions for "GEOSEARCH*" commands.
    """

    if not all_are_specified(longitude, latitude):
        raise Exception(
            """
            Both "longitude" and "latitude" must be specified.
            """
        )

    if not all_are_specified(width, height):
        raise Exception(
            """
            Both "width" and "height" must be specified.
            """
        )

    if not one_is_specified(member, longitude):
        raise Exception(
            """
            Specify either the member's name with "member", 
            or the longitude and latitude with "longitude" and "latitude", but not both.
            """
        )

    if not one_is_specified(radius, width):
        raise Exception(
            """
            Specify either the radius with "radius", 
            or the width and height with "width" and "height", but not both.
            """
        )

    if count_any and count is None:
        raise Exception(
            """
            "count_any" can only be used together with "count".
            """
        )


def handle_non_deprecated_zrange_exceptions(
    range_method: Literal["BYLEX", "BYSCORE"] | None,
    start: FloatMinMax,
    stop: FloatMinMax,
    offset: int | None,
    count: int | None,
) -> None:
    """
    Handle exceptions for non-deprecated "ZRANGE*" commands.
    """

    if range_method == "BYLEX" and (
            not start.startswith(("(", "[", "+inf", "-inf"))
            or not stop.startswith(("(", "[", "+inf", "-inf"))
    ):
        raise Exception(
            """
            "start" and "stop" must either start with "(" or "[" or be "+inf" or "-inf" when 
            the ranging method is "BYLEX".
            """
        )

    if not all_are_specified(offset, count):
        raise Exception(
            """
            Both "offset" and "count" must be specified.
            """
        )


def handle_zrangebylex_exceptions(
    min_score: str,
    max_score: str,
    offset: int | None,
    count: int | None,
) -> None:
    """
    Handle exceptions for "ZRANGEBYLEX" and "ZREVRANGEBYLEX" commands.
    """

    if not min_score.startswith(("(", "[", "+inf", "-inf")) or not max_score.startswith(("(", "[", "+inf", "-inf")):
        raise Exception(
            """
            "min_score" and "max_score" must either start with "(" or "[" or be "+inf" or "-inf".
            """
        )

    if not all_are_specified(offset, count):
        raise Exception(
            """
            Both "offset" and "count" must be specified.
            """
        )
