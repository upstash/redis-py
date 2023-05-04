from upstash_py.utils.base import base64_to_string


def test() -> None:
    assert base64_to_string("dGVzdA==") == "test"
