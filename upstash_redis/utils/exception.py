from upstash_redis.schema.commands.parameters import FloatMinMax
from upstash_redis.utils.comparison import number_are_not_none
from typing import Literal, Union


def handle_georadius_write_exceptions(
    withdist: bool = False,
    withhash: bool = False,
    withcoord: bool = False,
    count: Union[int, None] = None,
    count_any: bool = False,
    store: Union[str, None] = None,
    storedist: Union[str, None] = None,
) -> None:
    """
    Handle exceptions for "GEORADIUS*" write commands.
    """

    if count_any and count is None:
        raise Exception('"count_any" can only be used together with "count".')

    if (withdist or withhash or withcoord) and (store or storedist):
        raise Exception(
            'Cannot use "store" or "storedist" when requesting additional properties.'
        )


def handle_geosearch_exceptions(
    member: Union[str, None],
    fromlonlat_longitude: Union[float, None],
    fromlonlat_latitude: Union[float, None],
    byradius: Union[float, None],
    bybox_width: Union[float, None],
    bybox_height: Union[float, None],
    count: Union[int, None],
    count_any: bool,
) -> None:
    """
    Handle exceptions for "GEOSEARCH*" commands.
    """

    if number_are_not_none(fromlonlat_longitude, fromlonlat_latitude, number=1):
        raise Exception(
            'Both "fromlonlat_longitude" and "fromlonlat_latitude" must be specified.'
        )

    if number_are_not_none(bybox_width, bybox_height, number=1):
        raise Exception('Both "bybox_width" and "bybox_height" must be specified.')

    if not number_are_not_none(member, fromlonlat_longitude, number=1):
        raise Exception(
            """Specify either the member's name with "member",
or the fromlonlat_longitude and fromlonlat_latitude with "fromlonlat_longitude" and "fromlonlat_latitude", but not both."""
        )

    if not number_are_not_none(byradius, bybox_width, number=1):
        raise Exception(
            """Specify either the byradius with "byradius",
or the bybox_width and bybox_height with "bybox_width" and "bybox_height", but not both."""
        )

    if count_any and count is None:
        raise Exception('"count_any" can only be used together with "count".')


def handle_non_deprecated_zrange_exceptions(
    range_method: Union[Literal["BYLEX", "BYSCORE"], None],
    start: FloatMinMax,
    stop: FloatMinMax,
    offset: Union[int, None],
    count: Union[int, None],
) -> None:
    """
    Handle exceptions for non-deprecated "ZRANGE*" commands.
    """

    if range_method == "BYLEX" and (
        not (isinstance(start, str) and isinstance(stop, str))
        or not (
            start.startswith(("(", "[", "+", "-"))
            and stop.startswith(("(", "[", "+", "-"))
        )
    ):
        raise Exception(
            """"start" and "stop" must either start with '(' or '[' or be '+' or '-' when
the ranging method is "BYLEX"."""
        )

    if number_are_not_none(offset, count, number=1):
        raise Exception('Both "offset" and "count" must be specified.')


def handle_zrangebylex_exceptions(
    min_score: str,
    max_score: str,
    offset: Union[int, None],
    count: Union[int, None],
) -> None:
    """
    Handle exceptions for "ZRANGEBYLEX" and "ZREVRANGEBYLEX" commands.

    :param min_score: replacement for "MIN"
    :param max_score: replacement for "MAX"
    """

    if not min_score.startswith(("(", "[", "+", "-")) or not max_score.startswith(
        ("(", "[", "+", "-")
    ):
        raise Exception(
            "\"min_score\" and \"max_score\" must either start with '(' or '[' or be '+' or '-'."
        )

    if number_are_not_none(offset, count, number=1):
        raise Exception('Both "offset" and "count" must be specified.')
