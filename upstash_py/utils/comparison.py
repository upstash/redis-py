from typing import Any


def all_are_specified(*parameters: Any) -> bool:
    """
    Check if all the given parameters are specified.
    """

    return all(parameter is not None for parameter in parameters)


def one_is_specified(*parameters: Any) -> bool:
    """
    Check if only one of the given parameters is specified.
    """

    return sum(parameter is not None for parameter in parameters) == 1
