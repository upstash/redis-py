from upstash_redis.format import (
    format_float_list,
    format_geo_search_result,
    format_geopos,
    format_time,
    list_to_dict,
    string_to_json,
    string_list_to_json_list,
    list_to_optional_bool_list,
)
from upstash_redis.utils import GeoSearchResult


def test_list_to_dict() -> None:
    assert list_to_dict(raw=["a", "1", "b", "2", "c", 3]) == {
        "a": "1",
        "b": "2",
        "c": 3,
    }


def test_format_float_list() -> None:
    assert format_float_list(raw=["1.1", "2.2", None]) == [1.1, 2.2, None]


def test_string_to_json() -> None:
    assert string_to_json(res='{"a": 1, "b": 2}') == {"a": 1, "b": 2}


def test_string_list_to_json_list() -> None:
    assert string_list_to_json_list(res=['{"a": 1, "b": 2}', '{"c": 3, "d": 4}']) == [
        {"a": 1, "b": 2},
        {"c": 3, "d": 4},
    ]


def test_format_geo_members_with_distance_and_hash_and_coordinates() -> None:
    assert format_geo_search_result(
        raw=[
            ["a", "2.51", "100", ["3.12", "4.23"]],
            ["b", "5.6", "200", ["7.1", "8.2"]],
        ],
        with_distance=True,
        with_hash=True,
        with_coordinates=True,
    ) == [
        GeoSearchResult(
            member="a",
            distance=2.51,
            hash=100,
            longitude=3.12,
            latitude=4.23,
        ),
        GeoSearchResult(
            member="b",
            distance=5.6,
            hash=200,
            longitude=7.1,
            latitude=8.2,
        ),
    ]


def test_format_geo_members_with_distance() -> None:
    assert format_geo_search_result(
        raw=[
            ["a", "2.51"],
            ["b", "5.6"],
        ],
        with_distance=True,
        with_hash=False,
        with_coordinates=False,
    ) == [
        GeoSearchResult(member="a", distance=2.51),
        GeoSearchResult(member="b", distance=5.6),
    ]


def test_format_geo_members_with_hash() -> None:
    assert format_geo_search_result(
        raw=[
            ["a", "100"],
            ["b", "200"],
        ],
        with_distance=False,
        with_hash=True,
        with_coordinates=False,
    ) == [
        GeoSearchResult(member="a", hash=100),
        GeoSearchResult(member="b", hash=200),
    ]


def test_format_geo_members_with_coordinates() -> None:
    assert format_geo_search_result(
        raw=[
            ["a", ["3.12", "4.23"]],
            ["b", ["7.1", "8.2"]],
        ],
        with_distance=False,
        with_hash=False,
        with_coordinates=True,
    ) == [
        GeoSearchResult(member="a", longitude=3.12, latitude=4.23),
        GeoSearchResult(member="b", longitude=7.1, latitude=8.2),
    ]


def test_format_geo_members_with_distance_and_hash() -> None:
    assert format_geo_search_result(
        raw=[
            ["a", "2.51", "100"],
            ["b", "5.6", "200"],
        ],
        with_distance=True,
        with_hash=True,
        with_coordinates=False,
    ) == [
        GeoSearchResult(member="a", distance=2.51, hash=100),
        GeoSearchResult(member="b", distance=5.6, hash=200),
    ]


def test_format_geo_members_with_distance_and_coordinates() -> None:
    assert format_geo_search_result(
        raw=[
            ["a", "2.51", ["3.12", "4.23"]],
            ["b", "5.6", ["7.1", "8.2"]],
        ],
        with_distance=True,
        with_hash=False,
        with_coordinates=True,
    ) == [
        GeoSearchResult(member="a", distance=2.51, longitude=3.12, latitude=4.23),
        GeoSearchResult(member="b", distance=5.6, longitude=7.1, latitude=8.2),
    ]


def test_format_geo_members_with_hash_and_coordinates() -> None:
    assert format_geo_search_result(
        raw=[
            ["a", "100", ["3.12", "4.23"]],
            ["b", "200", ["7.1", "8.2"]],
        ],
        with_distance=False,
        with_hash=True,
        with_coordinates=True,
    ) == [
        GeoSearchResult(member="a", hash=100, longitude=3.12, latitude=4.23),
        GeoSearchResult(member="b", hash=200, longitude=7.1, latitude=8.2),
    ]


def test_format_geo_positions() -> None:
    assert format_geopos(raw=[["1.0", "2.5"], ["3.1", "4.2"], None]) == [
        (1.0, 2.5),
        (3.1, 4.2),
        None,
    ]


def test_format_server_time() -> None:
    assert format_time(raw=["1620752099", "12"]) == (1620752099, 12)


def test_list_to_optional_bool_list() -> None:
    assert list_to_optional_bool_list([1, 0, None, 1], None) == [
        True,
        False,
        None,
        True,
    ]
