from upstash_py.utils.format import format_float_list


def test() -> None:
    assert format_float_list(raw=["1.1", "2.2", None]) == [1.1, 2.2, None]
