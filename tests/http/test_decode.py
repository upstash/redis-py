from upstash_redis.http.execute import decode


def test() -> None:
    assert decode("dGVzdA==", "base64") == "test"


def test_with_ok() -> None:
    assert decode("OK", "base64") == "OK"


def test_with_integer() -> None:
    assert decode(1, "base64") == 1


def test_with_none() -> None:
    assert decode(None, "base64") is None


def test_with_list() -> None:
    assert decode(["YQ==", "YWJjZA==", 1, "MQ==", None], "base64") == [
        "a",
        "abcd",
        1,
        "1",
        None,
    ]


def test_with_2d_list() -> None:
    assert decode(
        ["YQ==", "YWJjZA==", 1, "MQ==", None, ["YQ==", "YWJjZA==", 1, "MQ==", None]],
        "base64",
    ) == ["a", "abcd", 1, "1", None, ["a", "abcd", 1, "1", None]]


def test_with_3d_list() -> None:
    assert decode(
        [
            "YQ==",
            "YWJjZA==",
            1,
            "MQ==",
            None,
            [
                "YQ==",
                "YWJjZA==",
                1,
                "MQ==",
                None,
                ["YQ==", "YWJjZA==", 1, "MQ==", None],
            ],
        ],
        "base64",
    ) == [
        "a",
        "abcd",
        1,
        "1",
        None,
        ["a", "abcd", 1, "1", None, ["a", "abcd", 1, "1", None]],
    ]
