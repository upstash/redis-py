from typing import Any


def number_are_not_none(*parameters: Any, number: int) -> bool:
    """
    Check if "number" of the given parameters are not None.
    """

    return sum(parameter is not None for parameter in parameters) == number
