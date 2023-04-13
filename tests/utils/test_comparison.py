from upstash_py.utils.comparison import all_are_specified, one_is_specified


def test_all_are_specified() -> None:
    assert all_are_specified(1, 2, 3)
    assert not all_are_specified(1, 2, None)
    assert not all_are_specified(None, None, None)


def test_one_is_specified() -> None:
    assert one_is_specified(2, None, None)
    assert not one_is_specified(1, 2, 3)
    assert not one_is_specified(None, None, None)
