from upstash_py.utils.format import format_bool_list


def test() -> None:
    assert format_bool_list(raw=[1, 0, 1, 1, 0]) == [True, False, True, True, False]
