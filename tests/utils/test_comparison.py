from upstash_py.utils.comparison import number_are_not_none


def test_number_are_not_none() -> None:
    assert number_are_not_none(2, None, None, number=1)

    assert not number_are_not_none(1, 2, 3, number=1)

    assert not number_are_not_none(None, None, None, number=1)

    assert number_are_not_none(1, 2, 3, number=3)

    assert number_are_not_none(None, None, None, number=0)
