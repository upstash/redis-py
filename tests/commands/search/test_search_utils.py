"""Unit tests for search utility functions."""

import pytest

from upstash_redis.search_utils import (
    flatten_schema,
    build_create_index_command,
    build_query_command,
    deserialize_query_response,
    deserialize_describe_response,
)


class TestFlattenSchema:
    """Tests for flatten_schema function."""

    def test_flatten_simple_schema(self):
        """Test flattening a simple schema."""
        schema = {"name": "TEXT", "age": {"type": "U64", "fast": True}, "active": "BOOL"}

        flattened = flatten_schema(schema)

        assert len(flattened) == 3
        assert flattened[0].path == "name"
        assert flattened[0].field_type == "TEXT"
        assert flattened[1].path == "age"
        assert flattened[1].field_type == "U64"
        assert flattened[1].fast is True

    def test_flatten_nested_schema(self):
        """Test flattening a nested schema."""
        schema = {
            "title": "TEXT",
            "author": {"name": "TEXT", "email": "TEXT"},
            "stats": {"views": {"type": "U64", "fast": True}},
        }

        flattened = flatten_schema(schema)

        assert len(flattened) == 4
        # Check that nested paths are created correctly
        paths = [f.path for f in flattened]
        assert "title" in paths
        assert "author.name" in paths
        assert "author.email" in paths
        assert "stats.views" in paths

    def test_flatten_with_field_options(self):
        """Test flattening schema with field options."""
        schema = {
            "title": {"type": "TEXT", "noStem": True, "noTokenize": True},
            "count": {"type": "U64", "fast": True},
        }

        flattened = flatten_schema(schema)

        title_field = next(f for f in flattened if f.path == "title")
        assert title_field.no_stem is True
        assert title_field.no_tokenize is True


class TestBuildCreateIndexCommand:
    """Tests for build_create_index_command function."""

    def test_build_simple_create_command(self):
        """Test building a simple create index command."""
        params = {
            "name": "test-idx",
            "dataType": "string",
            "prefix": "test:",
            "schema": {"name": "TEXT", "age": {"type": "U64", "fast": True}},
        }

        command = build_create_index_command(params)

        assert command == [
            "SEARCH.CREATE",
            "test-idx",
            "ON",
            "STRING",
            "PREFIX",
            "1",
            "test:",
            "SCHEMA",
            "name",
            "TEXT",
            "age",
            "U64",
            "FAST",
        ]

    def test_build_create_with_language(self):
        """Test building create command with language option."""
        params = {
            "name": "test-idx",
            "dataType": "json",
            "prefix": "test:",
            "schema": {"content": "TEXT"},
            "language": "turkish",
        }

        command = build_create_index_command(params)

        assert command == [
            "SEARCH.CREATE",
            "test-idx",
            "ON",
            "JSON",
            "PREFIX",
            "1",
            "test:",
            "LANGUAGE",
            "turkish",
            "SCHEMA",
            "content",
            "TEXT",
        ]

    def test_build_create_with_multiple_prefixes(self):
        """Test building create command with multiple prefixes."""
        params = {
            "name": "test-idx",
            "dataType": "hash",
            "prefix": ["user:", "profile:"],
            "schema": {"name": "TEXT"},
        }

        command = build_create_index_command(params)

        assert command == [
            "SEARCH.CREATE",
            "test-idx",
            "ON",
            "HASH",
            "PREFIX",
            "2",
            "user:",
            "profile:",
            "SCHEMA",
            "name",
            "TEXT",
        ]

    def test_build_create_with_skip_initial_scan(self):
        """Test building create command with skipInitialScan."""
        params = {
            "name": "test-idx",
            "dataType": "string",
            "prefix": "test:",
            "schema": {"name": "TEXT"},
            "skipInitialScan": True,
        }

        command = build_create_index_command(params)

        assert command == [
            "SEARCH.CREATE",
            "test-idx",
            "SKIPINITIALSCAN",
            "ON",
            "STRING",
            "PREFIX",
            "1",
            "test:",
            "SCHEMA",
            "name",
            "TEXT",
        ]

    def test_build_create_with_field_options(self):
        """Test building create command with field options."""
        params = {
            "name": "test-idx",
            "dataType": "string",
            "prefix": "test:",
            "schema": {
                "title": {"type": "TEXT", "noStem": True, "noTokenize": True},
                "score": {"type": "F64", "fast": True},
            },
        }

        command = build_create_index_command(params)

        assert command == [
            "SEARCH.CREATE",
            "test-idx",
            "ON",
            "STRING",
            "PREFIX",
            "1",
            "test:",
            "SCHEMA",
            "title",
            "TEXT",
            "NOTOKENIZE",
            "NOSTEM",
            "score",
            "F64",
            "FAST",
        ]


class TestBuildQueryCommand:
    """Tests for build_query_command function."""

    def test_build_simple_query(self):
        """Test building a simple query command."""
        command = build_query_command(
            "SEARCH.QUERY", "test-idx", {"filter": {"name": {"$eq": "test"}}}
        )

        assert command[0] == "SEARCH.QUERY"
        assert command[1] == "test-idx"
        assert '{"name":{"$eq":"test"}}' in command[2] or '{"name": {"$eq": "test"}}' in command[2]

    def test_build_query_with_limit(self):
        """Test building query with limit."""
        command = build_query_command(
            "SEARCH.QUERY", "test-idx", {"filter": {"name": {"$eq": "test"}}, "limit": 10}
        )

        assert command == [
            "SEARCH.QUERY",
            "test-idx",
            '{"name":{"$eq":"test"}}',
            "LIMIT",
            "10",
        ]

    def test_build_query_with_offset(self):
        """Test building query with offset."""
        command = build_query_command(
            "SEARCH.QUERY", "test-idx", {"filter": {"name": {"$eq": "test"}}, "limit": 10, "offset": 5}
        )

        assert command == [
            "SEARCH.QUERY",
            "test-idx",
            '{"name":{"$eq":"test"}}',
            "LIMIT",
            "10",
            "OFFSET",
            "5",
        ]

    def test_build_query_with_sorting(self):
        """Test building query with sorting."""
        command = build_query_command(
            "SEARCH.QUERY",
            "test-idx",
            {"filter": {"name": {"$eq": "test"}}, "orderBy": {"score": "DESC"}},
        )

        assert command == [
            "SEARCH.QUERY",
            "test-idx",
            '{"name":{"$eq":"test"}}',
            "SORTBY",
            "score",
            "DESC",
        ]

    def test_build_query_with_select_nocontent(self):
        """Test building query with noContent."""
        command = build_query_command(
            "SEARCH.QUERY", "test-idx", {"filter": {"name": {"$eq": "test"}}, "select": {}}
        )

        assert command == [
            "SEARCH.QUERY",
            "test-idx",
            '{"name":{"$eq":"test"}}',
            "NOCONTENT",
        ]

    def test_build_query_with_select_fields(self):
        """Test building query with specific return fields."""
        command = build_query_command(
            "SEARCH.QUERY",
            "test-idx",
            {"filter": {"name": {"$eq": "test"}}, "select": {"name": True, "age": True}},
        )

        assert command == [
            "SEARCH.QUERY",
            "test-idx",
            '{"name":{"$eq":"test"}}',
            "RETURN",
            "2",
            "name",
            "age",
        ]

    def test_build_query_numeric_filters(self):
        """Test building query with numeric filters."""
        # Test JSON format
        command = build_query_command("SEARCH.QUERY", "test-idx", {"filter": {"price": {"$gt": 100}}})
        assert command == [
            "SEARCH.QUERY",
            "test-idx",
            '{"price":{"$gt":100}}',
        ]

    def test_build_query_text_filters(self):
        """Test building query with text filters."""
        # Test JSON format
        command = build_query_command(
            "SEARCH.QUERY", "test-idx", {"filter": {"name": {"$fuzzy": "test"}}}
        )
        assert command == [
            "SEARCH.QUERY",
            "test-idx",
            '{"name":{"$fuzzy":"test"}}',
        ]


class TestDeserializeQueryResponse:
    """Tests for deserialize_query_response function."""

    def test_deserialize_simple_response(self):
        """Test deserializing a simple query response."""
        raw = [["key1", "0.5", [["name", "test"], ["age", 25]]]]

        result = deserialize_query_response(raw)

        assert len(result) == 1
        assert result[0]["key"] == "key1"
        assert result[0]["score"] == "0.5"
        assert "data" in result[0]

    def test_deserialize_nocontent_response(self):
        """Test deserializing a response with no content."""
        raw = [["key1", "0.5"]]

        result = deserialize_query_response(raw)

        assert len(result) == 1
        assert result[0]["key"] == "key1"
        assert result[0]["score"] == "0.5"
        assert "data" not in result[0]

    def test_deserialize_nested_fields(self):
        """Test deserializing response with nested fields."""
        raw = [["key1", "0.5", [["author.name", "John"], ["author.email", "john@example.com"]]]]

        result = deserialize_query_response(raw)

        assert len(result) == 1
        assert "data" in result[0]
        # The deserialization should create nested structure
        # (implementation may vary)


class TestDeserializeDescribeResponse:
    """Tests for deserialize_describe_response function."""

    def test_deserialize_describe(self):
        """Test deserializing a describe response."""
        raw = [
            "name",
            "test-idx",
            "type",
            "STRING",
            "prefixes",
            ["test:"],
            "schema",
            [["name", "TEXT"], ["age", "U64", "FAST"]],
        ]

        result = deserialize_describe_response(raw)

        assert result["name"] == "test-idx"
        assert result["dataType"] == "string"
        assert result["prefixes"] == ["test:"]
        assert "schema" in result
        assert "name" in result["schema"]
        assert result["schema"]["name"]["type"] == "TEXT"
