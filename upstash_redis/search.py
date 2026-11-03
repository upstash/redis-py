"""Type definitions for search index functionality."""

import dataclasses
import enum
import json
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union


# Data types that can be indexed
class DataType(str, enum.Enum):
    JSON = "JSON"
    HASH = "HASH"
    STRING = "STRING"
    STREAM = "STREAM"


class FieldType(str, enum.Enum):
    TEXT = "TEXT"
    U64 = "U64"
    I64 = "I64"
    F64 = "F64"
    BOOL = "BOOL"
    DATE = "DATE"
    KEYWORD = "KEYWORD"
    FACET = "FACET"
    # A TEXT field that is also embedded for semantic search with the $ann retriever.
    HYBRID = "HYBRID"


class FieldOptions(TypedDict, total=False):
    """Detailed field configuration with options."""

    type: Union[FieldType, str]
    fast: bool
    no_tokenize: bool
    no_stem: bool
    # Read the value from this document field (or nested path) instead of the schema key.
    alias: str


Schema = Dict[str, Union[Union[FieldType, str], FieldOptions]]


class Language(str, enum.Enum):
    ENGLISH = "english"
    ARABIC = "arabic"
    DANISH = "danish"
    DUTCH = "dutch"
    FINNISH = "finnish"
    FRENCH = "french"
    GERMAN = "german"
    GREEK = "greek"
    HUNGARIAN = "hungarian"
    ITALIAN = "italian"
    NORWEGIAN = "norwegian"
    PORTUGUESE = "portuguese"
    ROMANIAN = "romanian"
    RUSSIAN = "russian"
    SPANISH = "spanish"
    SWEDISH = "swedish"
    TAMIL = "tamil"
    TURKISH = "turkish"


class Order(str, enum.Enum):
    ASC = "ASC"
    DESC = "DESC"


class ScoreModifier(str, enum.Enum):
    """Score modifiers for field values."""

    NONE = "NONE"
    LOG = "LOG"
    LOG1P = "LOG1P"
    LOG2P = "LOG2P"
    LN = "LN"
    LN1P = "LN1P"
    LN2P = "LN2P"
    SQUARE = "SQUARE"
    SQRT = "SQRT"
    RECIPROCAL = "RECIPROCAL"


class ScoreMode(str, enum.Enum):
    """Score combination modes."""

    SUM = "SUM"
    MULTIPLY = "MULTIPLY"
    REPLACE = "REPLACE"


class CombineMode(str, enum.Enum):
    """Combine modes for multiple field values."""

    SUM = "SUM"
    MULTIPLY = "MULTIPLY"


class ScoreByField(TypedDict, total=False):
    """Score function configuration for a single field."""

    field: str
    modifier: ScoreModifier
    factor: Union[int, float]
    missing: Union[int, float]
    scoreMode: ScoreMode


class MultipleField(TypedDict, total=False):
    """Score function configuration for a field in multiple fields."""

    field: str
    modifier: ScoreModifier
    factor: Union[int, float]
    missing: Union[int, float]


class ScoreByMultipleFields(TypedDict, total=False):
    """Score function configuration for multiple fields."""

    fields: List[Union[str, MultipleField]]
    combineMode: CombineMode
    scoreMode: ScoreMode


ScoreFunc = Union[str, ScoreByField, ScoreByMultipleFields]


# Query options
class HighlightOptions(TypedDict, total=False):
    """Highlighting options for query results."""

    fields: List[str]
    tags: Tuple[str, str]  # "open" and "close" tags


# Query result
@dataclasses.dataclass
class QueryResult:
    """Result from a query operation."""

    key: str
    score: float
    data: Optional[Dict[str, Any]] = None


@dataclasses.dataclass
class CountResult:
    count: int


# Index description
@dataclasses.dataclass
class FieldInfo:
    """Field information from describe command."""

    type: FieldType
    alias: Optional[str]
    no_stem: bool
    no_tokenize: bool
    fast: bool


@dataclasses.dataclass
class IndexDescription:
    """Description of an index."""

    name: str
    data_type: DataType
    prefixes: List[str]
    language: Language
    schema: Dict[str, FieldInfo]


def deserialize_query_response(raw_response: List[Any]) -> List[QueryResult]:
    """
    Deserialize raw query response into structured results.

    Args:
        raw_response: Raw response from SEARCH.QUERY

    Returns:
        List of query results
    """
    results: List[QueryResult] = []

    for item in raw_response:
        key = item[0]
        score = float(item[1])
        data: Optional[Dict[str, Any]] = None

        if len(item) >= 3:
            data = {}
            for path, value in item[2]:
                path_parts = path.split(".")
                if len(path_parts) == 1:
                    data[path] = value
                else:
                    parent = data
                    for i, part in enumerate(path_parts):
                        if i < len(path_parts) - 1:
                            if part not in parent:
                                parent[part] = {}

                            parent = parent[part]
                        else:
                            parent[part] = value

            if "$" in data:
                data = json.loads(data["$"])

        result = QueryResult(key=key, score=score, data=data)
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

    name = ""
    data_type: DataType = DataType.JSON
    prefixes = []
    language = Language.ENGLISH
    schema: Dict[str, FieldInfo] = {}

    for i in range(0, len(raw_response), 2):
        descriptor = raw_response[i]
        if descriptor == "name":
            name = raw_response[i + 1]
        elif descriptor == "type":
            data_type = DataType[raw_response[i + 1].upper()]
        elif descriptor == "prefixes":
            prefixes = raw_response[i + 1]
        elif descriptor == "language":
            language = Language[raw_response[i + 1].upper()]
        elif descriptor == "schema":
            fields = raw_response[i + 1]
            for field_info in fields:
                field_name = field_info[0]
                field_type = field_info[1]

                alias: Optional[str] = None
                no_stem = False
                no_tokenize = False
                fast = False

                j = 2
                while j < len(field_info):
                    field_descriptor = field_info[j]
                    if field_descriptor == "FROM":
                        alias = field_info[j + 1]
                        j += 1
                    elif field_descriptor == "NOSTEM":
                        no_stem = True
                    elif field_descriptor == "NOTOKENIZE":
                        no_tokenize = True
                    elif field_descriptor == "FAST":
                        fast = True

                    j += 1

                schema[field_name] = FieldInfo(
                    type=field_type,
                    alias=alias,
                    no_stem=no_stem,
                    no_tokenize=no_tokenize,
                    fast=fast,
                )

    return IndexDescription(
        name=name,
        data_type=data_type,
        prefixes=prefixes,
        language=language,
        schema=schema,
    )


def _coerce_numeric_string(value: Any) -> Any:
    """Convert numeric strings to numbers."""
    if isinstance(value, str):
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value
    return value


def _parse_stats_value(arr: List[Any]) -> Dict[str, Any]:
    """Parse a stats-like flat key-value array into a dict."""
    result: Dict[str, Any] = {}
    for i in range(0, len(arr), 2):
        key = arr[i]
        value = arr[i + 1]

        if isinstance(value, list) and len(value) > 0:
            if isinstance(value[0], str):
                # Nested stats (e.g. stdDeviationBounds)
                result[key] = _parse_stats_value(value)
            elif (
                isinstance(value[0], list)
                and len(value[0]) > 0
                and isinstance(value[0][0], str)
            ):
                # Percentiles unkeyed: [[key, val, value, val], ...]
                result[key] = [_parse_stats_value(item) for item in value]
            else:
                result[key] = value
        else:
            result[key] = _coerce_numeric_string(value)

    return result


def _parse_pairs(arr: List[Any]) -> Dict[str, Any]:
    """Turn [name, value, name, value, ...] into a dict, keeping the values as they are."""
    return {arr[i]: arr[i + 1] for i in range(0, len(arr), 2)}


# Bucket values that are text even when they look like numbers
_TEXT_BUCKET_VALUES = ("key", "keyAsString", "fromAsString", "toAsString")


def _parse_bucket(bucket: List[Any]) -> Dict[str, Any]:
    bucket_obj: Dict[str, Any] = {}
    for i in range(0, len(bucket), 2):
        key = bucket[i]
        value = bucket[i + 1]
        if key == "key" and isinstance(value, list):
            # The key of a $composite bucket maps source names to values
            bucket_obj[key] = _parse_pairs(value)
        elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], str):
            # A sub-aggregation
            if value[0] == "buckets":
                bucket_obj[key] = _parse_buckets_value(value)
            else:
                bucket_obj[key] = _parse_stats_value(value)
        elif key in _TEXT_BUCKET_VALUES:
            # Keys are converted by _normalize_buckets, which knows the aggregation
            bucket_obj[key] = value
        else:
            # Counts and bounds such as docCount and to
            bucket_obj[key] = _coerce_numeric_string(value)
    return bucket_obj


def _parse_buckets_value(arr: List[Any]) -> Dict[str, Any]:
    """Parse a bucket aggregation value starting with 'buckets'."""
    if arr[0] == "buckets" and isinstance(arr[1], list):
        raw_buckets = arr[1]
        buckets: Any
        if len(raw_buckets) > 0 and not isinstance(raw_buckets[0], list):
            # keyed: true returns [key, bucket, key, bucket, ...]
            buckets = {
                raw_buckets[i]: _parse_bucket(raw_buckets[i + 1])
                for i in range(0, len(raw_buckets), 2)
            }
        else:
            buckets = [_parse_bucket(bucket) for bucket in raw_buckets]

        result: Dict[str, Any] = {"buckets": buckets}
        # Extra key-value pairs after the buckets array (sumOtherDocCount, afterKey, etc.)
        for i in range(2, len(arr), 2):
            if arr[i] == "afterKey" and isinstance(arr[i + 1], list):
                # The afterKey of $composite maps source names to cursor values
                result[arr[i]] = _parse_pairs(arr[i + 1])
            else:
                result[arr[i]] = _coerce_numeric_string(arr[i + 1])
        return result

    return {"raw": arr}


def _parse_aggregation_array(arr: List[Any]) -> Dict[str, Any]:
    """Parse a top-level aggregation response array."""
    result: Dict[str, Any] = {}
    for i in range(0, len(arr), 2):
        key = arr[i]
        value = arr[i + 1]

        if isinstance(value, list):
            if len(value) > 0 and isinstance(value[0], str):
                if value[0] == "buckets":
                    result[key] = _parse_buckets_value(value)
                else:
                    result[key] = _parse_stats_value(value)
            else:
                result[key] = _parse_aggregation_array(value)
        else:
            result[key] = value

    return result


def deserialize_aggregate_response(
    raw_response: Any, aggregations: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Deserialize raw aggregate response into structured results.

    Args:
        raw_response: Raw response from SEARCH.AGGREGATE
        aggregations: The aggregations of the request

    Returns:
        Parsed aggregation results
    """
    if not isinstance(raw_response, list) or len(raw_response) == 0:
        return {}

    result = _parse_aggregation_array(raw_response)
    if aggregations:
        _normalize_buckets(result, aggregations, nested=False)
    return result


# Aggregations whose bucket keys are always numbers
_NUMERIC_KEY_OPERATORS = ("$histogram", "$dateHistogram")


def _normalize_buckets(result: Any, aggregations: Dict[str, Any], nested: bool) -> None:
    """
    Fix buckets that only the request explains:
    - The server sends empty ``keyed`` buckets as a list.
    - Numbers arrive as strings. Histogram keys are always numbers. The field type of a
      $terms key is unknown, so nested $terms keys that look like numbers become numbers,
      as before, and top-level and $composite ones stay as they are.
    """
    if not isinstance(result, dict):
        return
    for name, aggregation in aggregations.items():
        value = result.get(name)
        if not isinstance(value, dict) or not isinstance(aggregation, dict):
            continue
        operator = next((key for key in aggregation if key != "$aggs"), None)
        params = aggregation.get(operator) if operator else None
        buckets = value.get("buckets")
        if isinstance(params, dict) and params.get("keyed") is True and buckets == []:
            buckets = value["buckets"] = {}
        if not isinstance(buckets, (dict, list)):
            continue

        numeric_sources: List[str] = []
        if operator == "$composite" and isinstance(params, dict):
            for source in params.get("sources") or []:
                for source_name, source_aggregation in (source or {}).items():
                    if any(
                        op in _NUMERIC_KEY_OPERATORS for op in source_aggregation or {}
                    ):
                        numeric_sources.append(source_name)

        sub_aggregations = aggregation.get("$aggs")
        for bucket in buckets.values() if isinstance(buckets, dict) else buckets:
            if not isinstance(bucket, dict):
                continue
            key = bucket.get("key")
            if isinstance(key, dict):
                for source_name in numeric_sources:
                    if source_name in key:
                        key[source_name] = _coerce_numeric_string(key[source_name])
            elif operator in _NUMERIC_KEY_OPERATORS or (
                nested and operator == "$terms"
            ):
                bucket["key"] = _coerce_numeric_string(key)
            if isinstance(sub_aggregations, dict):
                _normalize_buckets(bucket, sub_aggregations, nested=True)
