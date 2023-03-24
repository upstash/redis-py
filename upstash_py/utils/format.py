def format_geo(
    raw: list[str],
    with_distance: bool | None = None,
    with_hash: bool | None = None,
    with_coordinates: bool | None = None
) -> list[dict[str, float | int]]:
    """
    Format the raw output returned by some Geo commands.

    They generally return, if requested, in order:
     - the distance (float)
     - the hash (int)
     - the coordinates as:
        - the longitude
        - the latitude

    All represented as strings
    """
    result: list[dict[str, float | int]] = []

    for member in raw:
        formatted_member: dict[str, float | int] = {
            "member": member[0]
        }

        if with_distance:
            formatted_member["distance"] = float(member[1])

            if with_hash:
                formatted_member["hash"] = int(member[2])

                if with_coordinates:
                    formatted_member["longitude"] = float(member[3])
                    formatted_member["latitude"] = float(member[4])

            elif with_coordinates:
                formatted_member["longitude"] = float(member[2])
                formatted_member["latitude"] = float(member[3])

        elif with_hash:
            formatted_member["hash"] = int(member[1])

            if with_coordinates:
                formatted_member["longitude"] = float(member[2])
                formatted_member["latitude"] = float(member[3])

        elif with_coordinates:
            formatted_member["longitude"] = float(member[1])
            formatted_member["latitude"] = float(member[2])

        result.append(formatted_member)

    return result
