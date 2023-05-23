from upstash_redis.schema.commands.parameters import FloatMinMax
from upstash_redis.utils.comparison import number_are_not_none
from typing import Literal


def handle_georadius_write_exceptions(
    with_distance: bool = False,
    with_hash: bool = False,
    with_coordinates: bool = False,
    count: int | None = None,
    count_any: bool = False,
    store_as: str | None = None,
    store_dist_as: str | None = None,
) -> None:
    """
    Handle exceptions for "GEORADIUS*" write commands.
    """

    if count_any and count is None:
        raise Exception("\"count_any\" can only be used together with \"count\".")

    if (with_distance or with_hash or with_coordinates) and (store_as or store_dist_as):
        raise Exception("Cannot use \"store_as\" or \"store_dist_as\" when requesting additional properties.")


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

    if number_are_not_none(longitude, latitude, number=1):
        raise Exception("Both \"longitude\" and \"latitude\" must be specified.")

    if number_are_not_none(width, height, number=1):
        raise Exception("Both \"width\" and \"height\" must be specified.")

    if not number_are_not_none(member, longitude, number=1):
        raise Exception("""Specify either the member's name with "member",
or the longitude and latitude with "longitude" and "latitude", but not both.""")

    if not number_are_not_none(radius, width, number=1):
        raise Exception("""Specify either the radius with "radius",
or the width and height with "width" and "height", but not both.""")

    if count_any and count is None:
        raise Exception("\"count_any\" can only be used together with \"count\".")


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
        not (
            isinstance(start, str) and isinstance(stop, str)
        ) or not (
            start.startswith(('(', '[', '+', '-'))
            and stop.startswith(('(', '[', '+', '-'))
        )
    ):
        raise Exception(""""start" and "stop" must either start with '(' or '[' or be '+' or '-' when
the ranging method is "BYLEX".""")

    if number_are_not_none(offset, count, number=1):
        raise Exception("Both \"offset\" and \"count\" must be specified.")


def handle_zrangebylex_exceptions(
    min_score: str,
    max_score: str,
    offset: int | None,
    count: int | None,
) -> None:
    """
    Handle exceptions for "ZRANGEBYLEX" and "ZREVRANGEBYLEX" commands.
    """

    if not min_score.startswith(('(', '[', '+', '-')) or not max_score.startswith(('(', '[', '+', '-')):
        raise Exception(
            "\"min_score\" and \"max_score\" must either start with '(' or '[' or be '+' or '-'."
        )

    if number_are_not_none(offset, count, number=1):
        raise Exception("Both \"offset\" and \"count\" must be specified.")
