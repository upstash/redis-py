"""Utility functions for search index operations."""

from typing import Any, Dict, List, Union

from upstash_redis.search_types import (
    CreateIndexParams,
    DescribeFieldInfo,
    DetailedField,
    FieldType,
    FlatIndexSchema,
    IndexDescription,
    NestedIndexSchema,
    QueryOptions,
    QueryResult,
    TextFieldFilter,
    NumericFieldFilter,
    BooleanFieldFilter,
)


class FlattenedField:
    """Represents a flattened field from a nested schema."""

    def __init__(
        self,
        path: str,
        field_type: FieldType,
        fast: bool = False,
        no_tokenize: bool = False,
        no_stem: bool = False,
        from_: str = None,
    ):
        self.path = path
        self.field_type = field_type
        self.fast = fast
        self.no_tokenize = no_tokenize
        self.no_stem = no_stem
        self.from_ = from_


def _is_field_type(value: Any) -> bool:
    """Check if value is a valid field type."""
    return isinstance(value, str) and value in ["TEXT", "U64", "I64", "F64", "BOOL", "DATE"]


def _is_detailed_field(value: Any) -> bool:
    """Check if value is a detailed field configuration."""
    return isinstance(value, dict) and "type" in value and _is_field_type(value.get("type"))


def _is_nested_schema(value: Any) -> bool:
    """Check if value is a nested schema."""
    return isinstance(value, dict) and not _is_detailed_field(value) and not _is_field_type(value)


def flatten_schema(
    schema: Union[NestedIndexSchema, FlatIndexSchema], path_prefix: List[str] = None
) -> List[FlattenedField]:
    """
    Flatten a nested schema into a list of field definitions.

    Args:
        schema: The schema to flatten
        path_prefix: Current path prefix for nested fields

    Returns:
        List of flattened field definitions
    """
    if path_prefix is None:
        path_prefix = []

    fields: List[FlattenedField] = []

    for key, value in schema.items():
        current_path = path_prefix + [key]
        path_string = ".".join(current_path)

        if _is_field_type(value):
            fields.append(FlattenedField(path=path_string, field_type=value))
        elif _is_detailed_field(value):
            detailed = value
            fields.append(
                FlattenedField(
                    path=path_string,
                    field_type=detailed["type"],
                    fast=detailed.get("fast", False),
                    no_tokenize=detailed.get("noTokenize", False),
                    no_stem=detailed.get("noStem", False),
                    from_=detailed.get("from_"),
                )
            )
        elif _is_nested_schema(value):
            nested_fields = flatten_schema(value, current_path)
            fields.extend(nested_fields)

    return fields


def build_create_index_command(params: CreateIndexParams) -> List[str]:
    """
    Build the SEARCH.CREATE command from parameters.

    Args:
        params: Create index parameters

    Returns:
        Command list ready to execute
    """
    command: List[str] = ["SEARCH.CREATE", params["name"]]

    # Add options BEFORE "ON" keyword
    if params.get("skipInitialScan", False):
        command.append("SKIPINITIALSCAN")

    if params.get("existsOk", False):
        command.append("EXISTSOK")

    # Add "ON" keyword and data type
    command.append("ON")
    data_type = params["dataType"].upper()
    command.append(data_type)

    # Add prefixes
    prefixes = params["prefix"]
    if isinstance(prefixes, str):
        prefixes = [prefixes]

    command.append("PREFIX")
    command.append(str(len(prefixes)))
    command.extend(prefixes)

    # Add language if specified
    if "language" in params:
        command.extend(["LANGUAGE", params["language"]])

    # Add schema
    schema = params["schema"]
    flattened = flatten_schema(schema)

    command.append("SCHEMA")
    for field in flattened:
        # Add field path or from source
        if field.from_:
            command.extend(["AS", field.path, "FROM", field.from_])
        else:
            command.append(field.path)

        # Add field type
        command.append(field.field_type)

        # Add field options
        if field.no_tokenize:
            command.append("NOTOKENIZE")
        if field.no_stem:
            command.append("NOSTEM")
        if field.fast:
            command.append("FAST")

    return command


def _build_filter_query(filter_dict: Dict[str, Any]) -> List[str]:
    """
    Build filter query from filter dictionary.

    Args:
        filter_dict: Filter specification

    Returns:
        List of filter query parts
    """
    filters: List[str] = []

    for field_name, field_filter in filter_dict.items():
        if not isinstance(field_filter, dict):
            continue

        # Text filters
        if "eq" in field_filter:
            value = field_filter["eq"]
            if isinstance(value, bool):
                filters.append(f"@{field_name}:[{str(value).lower()}]")
            else:
                filters.append(f"@{field_name}:({value})")

        elif "fuzzy" in field_filter:
            fuzzy = field_filter["fuzzy"]
            if isinstance(fuzzy, str):
                filters.append(f"@{field_name}:%{fuzzy}%")
            elif isinstance(fuzzy, dict):
                value = fuzzy.get("value", "")
                distance = fuzzy.get("distance", 1)
                filters.append(f"@{field_name}:%" + "%" * distance + value + "%" * distance)

        elif "phrase" in field_filter:
            phrase = field_filter["phrase"]
            if isinstance(phrase, str):
                filters.append(f'@{field_name}:"{phrase}"')
            elif isinstance(phrase, dict):
                value = phrase.get("value", "")
                prefix = phrase.get("prefix", False)
                if prefix:
                    filters.append(f'@{field_name}:"{value}"*')
                else:
                    filters.append(f'@{field_name}:"{value}"')

        elif "regex" in field_filter:
            regex = field_filter["regex"]
            filters.append(f"@{field_name}:/{regex}/")

        # Numeric filters
        elif "gt" in field_filter or "gte" in field_filter or "lt" in field_filter or "lte" in field_filter:
            min_val = "-inf"
            max_val = "+inf"

            if "gt" in field_filter:
                min_val = f"({field_filter['gt']}"
            elif "gte" in field_filter:
                min_val = str(field_filter["gte"])

            if "lt" in field_filter:
                max_val = f"({field_filter['lt']}"
            elif "lte" in field_filter:
                max_val = str(field_filter["lte"])

            filters.append(f"@{field_name}:[{min_val} {max_val}]")

    return filters


def build_query_command(
    command_name: str, index_name: str, options: QueryOptions = None
) -> List[str]:
    """
    Build SEARCH.QUERY or SEARCH.COUNT command.

    Args:
        command_name: "SEARCH.QUERY" or "SEARCH.COUNT"
        index_name: Name of the index
        options: Query options

    Returns:
        Command list ready to execute
    """
    import json

    command: List[str] = [command_name, index_name]

    # Serialize filter to JSON (compact format without spaces)
    filter_json = "{}"
    if options and "filter" in options:
        filter_json = json.dumps(options["filter"], separators=(',', ':'))

    command.append(filter_json)

    if not options:
        return command

    # Add limit
    if "limit" in options:
        command.extend(["LIMIT", str(options["limit"])])

    # Add offset
    if "offset" in options:
        command.extend(["OFFSET", str(options["offset"])])

    # Add orderBy
    if "orderBy" in options:
        for field, direction in options["orderBy"].items():
            command.extend(["SORTBY", field, direction])

    # Add highlight
    if "highlight" in options:
        highlight = options["highlight"]
        command.append("HIGHLIGHT")
        if "fields" in highlight:
            fields = highlight["fields"]
            command.extend(["FIELDS", str(len(fields))])
            command.extend(fields)
        if "preTag" in highlight and "postTag" in highlight:
            command.extend(["TAGS", highlight["preTag"], highlight["postTag"]])

    # Add select
    if "select" in options:
        select = options["select"]
        if not select:  # Empty dict means NOCONTENT
            command.append("NOCONTENT")
        else:
            command.append("RETURN")
            fields = [f for f, include in select.items() if include]
            command.append(str(len(fields)))
            command.extend(fields)

    return command

    # Add highlight
    if "highlight" in options:
        highlight = options["highlight"]
        command.append("HIGHLIGHT")

        if "fields" in highlight and highlight["fields"]:
            command.append("FIELDS")
            command.append(str(len(highlight["fields"])))
            command.extend(highlight["fields"])

        if "tags" in highlight:
            tags = highlight["tags"]
            if "open" in tags and "close" in tags:
                command.extend(["TAGS", tags["open"], tags["close"]])

    return command


def deserialize_query_response(raw_response: List[Any]) -> List[QueryResult]:
    """
    Deserialize raw query response into structured results.

    Args:
        raw_response: Raw response from SEARCH.QUERY

    Returns:
        List of query results
    """
    results: List[QueryResult] = []

    for item_raw in raw_response:
        if not isinstance(item_raw, (list, tuple)) or len(item_raw) < 2:
            continue

        key = item_raw[0]
        score = item_raw[1]

        result: QueryResult = {"key": key, "score": score}

        if len(item_raw) > 2:
            raw_fields = item_raw[2]

            if isinstance(raw_fields, (list, tuple)) and len(raw_fields) > 0:
                data: Dict[str, Any] = {}

                # Process field pairs
                for field_raw in raw_fields:
                    if not isinstance(field_raw, (list, tuple)) or len(field_raw) < 2:
                        continue

                    field_key = field_raw[0]
                    field_value = field_raw[1]

                    # Handle nested paths
                    path_parts = field_key.split(".")
                    if len(path_parts) == 1:
                        data[field_key] = field_value
                    else:
                        # Build nested structure
                        current_obj = data
                        for i, part in enumerate(path_parts[:-1]):
                            if part not in current_obj:
                                current_obj[part] = {}
                            current_obj = current_obj[part]
                        current_obj[path_parts[-1]] = field_value

                # If $ key exists (full document), use its contents
                if "$" in data:
                    data = data["$"]

                result["data"] = data

        results.append(result)

    return results


def deserialize_describe_response(raw_response: List[Any]) -> IndexDescription:
    """
    Deserialize raw describe response into index description.

    Args:
        raw_response: Raw response from SEARCH.DESCRIBE

    Returns:
        Index description
    """
    description: IndexDescription = {}

    i = 0
    while i < len(raw_response):
        descriptor = raw_response[i]
        value = raw_response[i + 1] if i + 1 < len(raw_response) else None

        if descriptor == "name":
            description["name"] = value
        elif descriptor == "type":
            if value:
                description["dataType"] = value.lower()
        elif descriptor == "prefixes":
            description["prefixes"] = value if isinstance(value, list) else []
        elif descriptor == "language":
            description["language"] = value
        elif descriptor == "schema":
            schema: Dict[str, DescribeFieldInfo] = {}
            if isinstance(value, list):
                for field_desc in value:
                    if not isinstance(field_desc, list) or len(field_desc) < 2:
                        continue

                    field_name = field_desc[0]
                    field_info: DescribeFieldInfo = {"type": field_desc[1]}

                    # Parse field options
                    for j in range(2, len(field_desc)):
                        option = field_desc[j]
                        if option == "NOSTEM":
                            field_info["noStem"] = True
                        elif option == "NOTOKENIZE":
                            field_info["noTokenize"] = True
                        elif option == "FAST":
                            field_info["fast"] = True

                    schema[field_name] = field_info

            description["schema"] = schema

        i += 2

    return description


def parse_count_response(raw_response: Any) -> int:
    """
    Parse count response.

    Args:
        raw_response: Raw response from SEARCH.COUNT

    Returns:
        Count as integer
    """
    if isinstance(raw_response, int):
        return raw_response
    return int(raw_response)
