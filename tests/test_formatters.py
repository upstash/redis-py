from upstash_redis.format import (
    _list_to_dict,
    format_bool_list,
    format_float_list,
    format_geo_members_return,
    format_geo_positions_return,
    format_server_time_return,
)


def test_list_to_dict() -> None:
    assert _list_to_dict(raw=["a", "1", "b", "2", "c", 3]) == {
        "a": "1",
        "b": "2",
        "c": 3,
    }


def test_format_bool_list() -> None:
    assert format_bool_list(raw=[1, 0, 1, 1, 0]) == [True, False, True, True, False]


def test_format_float_list() -> None:
    assert format_float_list(raw=["1.1", "2.2", None]) == [1.1, 2.2, None]


def test_format_geo_members_with_distance_and_hash_and_coordinates() -> None:
    assert format_geo_members_return(
        raw=[
            ["a", "2.51", "100", ["3.12", "4.23"]],
            ["b", "5.6", "200", ["7.1", "8.2"]],
        ],
        with_distance=True,
        with_hash=True,
        with_coordinates=True,
    ) == [
        {
            "member": "a",
            "distance": 2.51,
            "hash": 100,
            "longitude": 3.12,
            "latitude": 4.23,
        },
        {
            "member": "b",
            "distance": 5.6,
            "hash": 200,
            "longitude": 7.1,
            "latitude": 8.2,
        },
    ]


def test_format_geo_members_with_distance() -> None:
    assert format_geo_members_return(
        raw=[
            ["a", "2.51"],
            ["b", "5.6"],
        ],
        with_distance=True,
        with_hash=False,
        with_coordinates=False,
    ) == [
        {"member": "a", "distance": 2.51},
        {"member": "b", "distance": 5.6},
    ]


def test_format_geo_members_with_hash() -> None:
    assert format_geo_members_return(
        raw=[
            ["a", "100"],
            ["b", "200"],
        ],
        with_distance=False,
        with_hash=True,
        with_coordinates=False,
    ) == [
        {"member": "a", "hash": 100},
        {"member": "b", "hash": 200},
    ]


def test_format_geo_members_with_coordinates() -> None:
    assert format_geo_members_return(
        raw=[
            ["a", ["3.12", "4.23"]],
            ["b", ["7.1", "8.2"]],
        ],
        with_distance=False,
        with_hash=False,
        with_coordinates=True,
    ) == [
        {"member": "a", "longitude": 3.12, "latitude": 4.23},
        {"member": "b", "longitude": 7.1, "latitude": 8.2},
    ]


def test_format_geo_members_with_distance_and_hash() -> None:
    assert format_geo_members_return(
        raw=[
            ["a", "2.51", "100"],
            ["b", "5.6", "200"],
        ],
        with_distance=True,
        with_hash=True,
        with_coordinates=False,
    ) == [
        {"member": "a", "distance": 2.51, "hash": 100},
        {"member": "b", "distance": 5.6, "hash": 200},
    ]


def test_format_geo_members_with_distance_and_coordinates() -> None:
    assert format_geo_members_return(
        raw=[
            ["a", "2.51", ["3.12", "4.23"]],
            ["b", "5.6", ["7.1", "8.2"]],
        ],
        with_distance=True,
        with_hash=False,
        with_coordinates=True,
    ) == [
        {"member": "a", "distance": 2.51, "longitude": 3.12, "latitude": 4.23},
        {"member": "b", "distance": 5.6, "longitude": 7.1, "latitude": 8.2},
    ]


def test_format_geo_members_with_hash_and_coordinates() -> None:
    assert format_geo_members_return(
        raw=[
            ["a", "100", ["3.12", "4.23"]],
            ["b", "200", ["7.1", "8.2"]],
        ],
        with_distance=False,
        with_hash=True,
        with_coordinates=True,
    ) == [
        {"member": "a", "hash": 100, "longitude": 3.12, "latitude": 4.23},
        {"member": "b", "hash": 200, "longitude": 7.1, "latitude": 8.2},
    ]


def test_format_geo_positions() -> None:
    assert format_geo_positions_return(raw=[["1.0", "2.5"], ["3.1", "4.2"], None]) == [
        {"longitude": 1.0, "latitude": 2.5},
        {"longitude": 3.1, "latitude": 4.2},
        None,
    ]


def test_format_server_time() -> None:
    assert format_server_time_return(raw=["1620752099", "12"]) == {
        "seconds": 1620752099,
        "microseconds": 12,
    }
