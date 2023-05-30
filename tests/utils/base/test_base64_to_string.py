from upstash_redis.utils.base import base64_to_string


def test() -> None:
    assert base64_to_string("dGVzdA==") == "test"
