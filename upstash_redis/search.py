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


class FieldType(str, enum.Enum):
    TEXT = "TEXT"
    U64 = "U64"
    I64 = "I64"
    F64 = "F64"
    BOOL = "BOOL"
    DATE = "DATE"


class FieldOptions(TypedDict, total=False):
    """Detailed field configuration with options."""

    type: Union[FieldType, str]
    fast: bool
    no_tokenize: bool
    no_stem: bool
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
