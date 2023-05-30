from upstash_redis.utils.format import format_geo_positions_return


def test() -> None:
    assert format_geo_positions_return(raw=[["1.0", "2.5"], ["3.1", "4.2"], None]) == [
        {"longitude": 1.0, "latitude": 2.5},
        {"longitude": 3.1, "latitude": 4.2},
        None
    ]