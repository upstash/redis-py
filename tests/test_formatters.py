from upstash_redis.format import (
    to_optional_float_list,
    format_geo_search_response,
    format_geopos,
    format_time,
    to_dict,
    string_to_json,
    to_json_list,
    to_optional_bool_list,
    format_search_query_response,
    format_search_describe_response,
    format_search_count_response,
)
from upstash_redis.utils import GeoSearchResult


def test_list_to_dict() -> None:
    assert to_dict(["a", "1", "b", "2", "c", 3], None) == {
        "a": "1",
        "b": "2",
        "c": 3,
    }


def test_format_float_list() -> None:
    assert to_optional_float_list(["1.1", "2.2", None], None) == [1.1, 2.2, None]


def test_string_to_json() -> None:
    assert string_to_json('{"a": 1, "b": 2}', None) == {"a": 1, "b": 2}


def test_string_list_to_json_list() -> None:
    assert to_json_list(['{"a": 1, "b": 2}', '{"c": 3, "d": 4}'], None) == [
        {"a": 1, "b": 2},
        {"c": 3, "d": 4},
    ]


def test_format_geo_members_with_distance_and_hash_and_coordinates() -> None:
    assert format_geo_search_response(
        [
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
    assert format_geo_search_response(
        [
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
    assert format_geo_search_response(
        [
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
    assert format_geo_search_response(
        [
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
    assert format_geo_search_response(
        [
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
    assert format_geo_search_response(
        [
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
    assert format_geo_search_response(
        [
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
    assert format_geopos([["1.0", "2.5"], ["3.1", "4.2"], None], None) == [
        (1.0, 2.5),
        (3.1, 4.2),
        None,
    ]


def test_format_server_time() -> None:
    assert format_time(["1620752099", "12"], None) == (1620752099, 12)


def test_list_to_optional_bool_list() -> None:
    assert to_optional_bool_list([1, 0, None, 1], None) == [
        True,
        False,
        None,
        True,
    ]


def test_format_search_query_response() -> None:
    # Test basic query response
    raw_response = [
        ["doc:1", 0.95, [["name", "Laptop"], ["price", "999"]]],
        ["doc:2", 0.85, [["name", "Phone"], ["price", "699"]]],
    ]
    result = format_search_query_response(raw_response, None)
    
    assert len(result) == 2
    assert result[0]["key"] == "doc:1"
    assert result[0]["score"] == 0.95
    assert result[0]["data"]["name"] == "Laptop"
    assert result[0]["data"]["price"] == "999"
    assert result[1]["key"] == "doc:2"
    assert result[1]["score"] == 0.85
    assert result[1]["data"]["name"] == "Phone"
    assert result[1]["data"]["price"] == "699"


def test_format_search_query_response_with_nested_paths() -> None:
    # Test query response with nested paths
    raw_response = [
        ["doc:1", 0.95, [["user.name", "John"], ["user.age", "30"]]],
    ]
    result = format_search_query_response(raw_response, None)
    
    assert len(result) == 1
    assert result[0]["key"] == "doc:1"
    assert result[0]["score"] == 0.95
    assert result[0]["data"]["user"]["name"] == "John"
    assert result[0]["data"]["user"]["age"] == "30"


def test_format_search_query_response_with_dollar_key() -> None:
    # Test query response with $ key (full document)
    raw_response = [
        ["doc:1", 1.0, [["$", {"name": "Laptop", "price": 999}]]],
    ]
    result = format_search_query_response(raw_response, None)
    
    assert len(result) == 1
    assert result[0]["key"] == "doc:1"
    assert result[0]["score"] == 1.0
    assert result[0]["data"]["name"] == "Laptop"
    assert result[0]["data"]["price"] == 999


def test_format_search_query_response_without_fields() -> None:
    # Test query response without field data
    raw_response = [
        ["doc:1", 0.95],
        ["doc:2", 0.85],
    ]
    result = format_search_query_response(raw_response, None)
    
    assert len(result) == 2
    assert result[0]["key"] == "doc:1"
    assert result[0]["score"] == 0.95
    assert "data" not in result[0]
    assert result[1]["key"] == "doc:2"
    assert result[1]["score"] == 0.85
    assert "data" not in result[1]


def test_format_search_describe_response() -> None:
    # Test describe response
    raw_response = [
        "name", "myindex",
        "type", "JSON",
        "prefixes", ["product:", "item:"],
        "language", "english",
        "schema", [
            ["name", "TEXT", "NOSTEM"],
            ["price", "F64", "FAST"],
            ["active", "BOOL"],
        ],
    ]
    result = format_search_describe_response(raw_response, None)
    
    assert result["name"] == "myindex"
    assert result["dataType"] == "json"
    assert result["prefixes"] == ["product:", "item:"]
    assert result["language"] == "english"
    assert "name" in result["schema"]
    assert result["schema"]["name"]["type"] == "TEXT"
    assert result["schema"]["name"]["noStem"] is True
    assert "price" in result["schema"]
    assert result["schema"]["price"]["type"] == "F64"
    assert result["schema"]["price"]["fast"] is True
    assert "active" in result["schema"]
    assert result["schema"]["active"]["type"] == "BOOL"


def test_format_search_describe_response_with_all_options() -> None:
    # Test describe response with all field options
    raw_response = [
        "name", "fullindex",
        "type", "HASH",
        "schema", [
            ["title", "TEXT", "NOSTEM", "NOTOKENIZE"],
            ["score", "I64", "FAST"],
        ],
    ]
    result = format_search_describe_response(raw_response, None)
    
    assert result["name"] == "fullindex"
    assert result["dataType"] == "hash"
    assert result["schema"]["title"]["type"] == "TEXT"
    assert result["schema"]["title"]["noStem"] is True
    assert result["schema"]["title"]["noTokenize"] is True
    assert result["schema"]["score"]["type"] == "I64"
    assert result["schema"]["score"]["fast"] is True


def test_format_search_count_response() -> None:
    # Test count response with int
    assert format_search_count_response(42, None) == {"count": 42}
    
    # Test count response with string
    assert format_search_count_response("100", None) == {"count": 100}
