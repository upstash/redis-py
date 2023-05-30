from upstash_redis.utils.comparison import number_are_not_none


def test_with_one_not_none_parameter() -> None:
    assert number_are_not_none(2, None, None, number=1)


def test_with_all_not_none_parameters() -> None:
    assert number_are_not_none(1, 2, 3, number=3)


def test_with_all_none_parameters() -> None:
    assert number_are_not_none(None, None, None, number=0)


def test_with_more_than_number_not_none_parameters() -> None:
    assert not number_are_not_none(1, 2, 3, number=1)


def test_with_all_none_parameters_and_positive_number() -> None:
    assert not number_are_not_none(None, None, None, number=1)
