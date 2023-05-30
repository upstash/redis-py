from upstash_redis.utils.format import format_server_time_return


def test() -> None:
    assert format_server_time_return(raw=["1620752099", "12"]) == {
        "seconds": 1620752099,
        "microseconds": 12
    }
