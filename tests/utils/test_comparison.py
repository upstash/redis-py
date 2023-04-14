from upstash_py.utils.comparison import one_is_specified


def test_one_is_specified() -> None:
    assert one_is_specified(2, None, None)
    assert not one_is_specified(1, 2, 3)
    assert not one_is_specified(None, None, None)
