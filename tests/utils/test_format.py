from upstash_py.utils.format import (
    _list_to_dict,
    format_geo_positions_return,
    format_geo_members_return,
    format_hash_return,
    format_pubsub_numsub_return,
    format_bool_list,
    format_server_time_return,
    format_sorted_set_return,
    format_float_list
)


def test__list_to_dict() -> None:
    assert _list_to_dict(raw=["a", "1", "b", "2", "c", "3"]) == {"a": "1", "b": "2", "c": "3"}


def test_format_geo_positions_return() -> None:
    assert format_geo_positions_return(raw=[["1.0", "2.5"], ["3.1", "4.2"], None]) == [
        {"longitude": 1.0, "latitude": 2.5},
        {"longitude": 3.1, "latitude": 4.2},
        None
    ]


def test_format_geo_members_return() -> None:
    # Test with distance, hash and coordinates.
    pass
