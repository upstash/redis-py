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
    if (
        member is not None
        and longitude is not None
        and latitude is not None
    ) or (
        member is None
        and longitude is None
        and latitude is None
    ):
        raise Exception(
            """
            Specify either the member's name with "member", 
            or the longitude and latitude with "longitude" and "latitude", but not both.
            """
        )

    if (
        radius is not None
        and width is not None
        and height is not None
    ) or (
        radius is None
        and width is None
        and height is None
    ):
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

