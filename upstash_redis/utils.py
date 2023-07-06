from typing import Any, Literal, Union

from upstash_redis.typing import FloatMinMaxT


def number_are_not_none(*parameters: Any, number: int) -> bool:
    """
    Check if "number" of the given parameters are not None.
    """

    return sum(parameter is not None for parameter in parameters) == number


def handle_georadius_write_exceptions(
    withdist: bool = False,
    withhash: bool = False,
    withcoord: bool = False,
    count: Union[int, None] = None,
    any: bool = False,
    store: Union[str, None] = None,
    storedist: Union[str, None] = None,
) -> None:
    """
    Handle exceptions for "GEORADIUS*" write commands.
    """

    if any and count is None:
        raise Exception('"any" can only be used together with "count".')

    if (withdist or withhash or withcoord) and (store or storedist):
        raise Exception(
            'Cannot use "store" or "storedist" when requesting additional properties.'
        )


def handle_geosearch_exceptions(
    member: Union[str, None],
    longitude: Union[float, None],
    latitude: Union[float, None],
    radius: Union[float, None],
    width: Union[float, None],
    height: Union[float, None],
    count: Union[int, None],
    any: bool,
) -> None:
    """
    Handle exceptions for "GEOSEARCH*" commands.
    """

    if number_are_not_none(longitude, latitude, number=1):
        raise Exception('Both "longitude" and "latitude" must be specified.')

    if number_are_not_none(width, height, number=1):
        raise Exception('Both "width" and "height" must be specified.')

    if not number_are_not_none(member, longitude, number=1):
        raise Exception(
            """Specify either the member's name with "member", or the "longitude" and "latitude", but not both."""
        )

    if not number_are_not_none(radius, width, number=1):
        raise Exception(
            """Specify either the "radius", or the "width" and "height", but not both."""
        )

    if any and count is None:
        raise Exception('"any" can only be used together with "count".')


def handle_non_deprecated_zrange_exceptions(
    sortby: Union[Literal["BYLEX", "BYSCORE"], None],
    start: FloatMinMaxT,
    stop: FloatMinMaxT,
    offset: Union[int, None],
    count: Union[int, None],
) -> None:
    """
    Handle exceptions for non-deprecated "ZRANGE*" commands.
    """

    if sortby == "BYLEX" and (
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
    min: str,
    max: str,
    offset: Union[int, None],
    count: Union[int, None],
) -> None:
    """
    Handle exceptions for "ZRANGEBYLEX" and "ZREVRANGEBYLEX" commands.
    """

    if not min.startswith(("(", "[", "+", "-")) or not max.startswith(
        ("(", "[", "+", "-")
    ):
        raise Exception(
            "\"min\" and \"max\" must either start with '(' or '[' or be '+' or '-'."
        )

    if number_are_not_none(offset, count, number=1):
        raise Exception('Both "offset" and "count" must be specified.')
