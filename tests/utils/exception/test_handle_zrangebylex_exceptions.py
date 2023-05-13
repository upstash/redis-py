from upstash_py.utils.exception import handle_zrangebylex_exceptions
from pytest import raises


def test_if_min_and_max_score_are_incorrectly_formatted() -> None:
    with raises(Exception) as exception:
        handle_zrangebylex_exceptions(
            min_score="0",
            max_score="1",
            offset=None,
            count=None,
        )

    assert str(exception.value) == """"min_score" and "max_score" must either start with '(' or '[' or be '+' or '-'."""


def test_with_invalid_offset_and_count() -> None:
    with raises(Exception) as exception:
        handle_zrangebylex_exceptions(
            min_score="(",
            max_score="[",
            offset=None,
            count=1,
        )

    assert str(exception.value) == "Both \"offset\" and \"count\" must be specified."


def test_with_parenthesis_min_and_max_score() -> None:
    handle_zrangebylex_exceptions(
        min_score="[",
        max_score="(",
        offset=None,
        count=None,
    )


def test_with_plus_and_minus_min_and_max_score() -> None:
    handle_zrangebylex_exceptions(
        min_score="-",
        max_score="+",
        offset=None,
        count=None,
    )


def test_with_offset_and_count() -> None:
    handle_zrangebylex_exceptions(
        min_score="(",
        max_score="[",
        offset=0,
        count=1,
    )
