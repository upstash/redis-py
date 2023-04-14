from typing import Any


def one_is_specified(*parameters: Any) -> bool:
    """
    Check if only one of the given parameters is specified.
    """

    return sum(parameter is not None for parameter in parameters) == 1
