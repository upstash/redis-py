from upstash_py.http.execute import decode
from tests.base import string_to_base64


def test_decode() -> None:
    assert decode("OK", "base64") == "OK"

    assert decode(string_to_base64("test"), "base64") == "test"

    assert decode(1, "base64") == 1

    assert decode(None, "base64") is None

    # Try to decode a 2D list.
    assert decode([
        string_to_base64(element) if isinstance(element, str)

        else [
            string_to_base64(member) if isinstance(member, str)

            else member

            for member in element
        ] if isinstance(element, list)

        else element

        for element in ["a", "abcd", 1, "1", None, ["a", "abcd", 1, "1", None]]
    ], "base64"
    ) == ["a", "abcd", 1, "1", None, ["a", "abcd", 1, "1", None]]