from upstash_py.http.execute import decode


def test_decode() -> None:
    assert decode("OK", "base64") == "OK"

    assert decode("dGVzdA==", "base64") == "test"

    assert decode(1, "base64") == 1

    assert decode(None, "base64") is None

    # Try to decode a 2D list.
    assert decode(
        [
            "YQ==",
            "YWJjZA==",
            1, "MQ==",
            None,
            ["YQ==", "YWJjZA==", 1, "MQ==", None]
        ],
        "base64"
    ) == ["a", "abcd", 1, "1", None, ["a", "abcd", 1, "1", None]]

