from upstash_py.utils.format import _list_to_dict


def test() -> None:
    assert _list_to_dict(raw=["a", "1", "b", "2", "c", 3]) == {"a": "1", "b": "2", "c": 3}
