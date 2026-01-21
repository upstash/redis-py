"""Type definitions for search index functionality."""

from typing import Any, Dict, List, Literal, Optional, TypedDict, Union

# Field types
FieldType = Literal["TEXT", "U64", "I64", "F64", "BOOL", "DATE"]

# Language options for text indexing
Language = Literal[
    "arabic",
    "basque",
    "catalan",
    "danish",
    "dutch",
    "english",
    "finnish",
    "french",
    "german",
    "greek",
    "hungarian",
    "indonesian",
    "irish",
    "italian",
    "lithuanian",
    "nepali",
    "norwegian",
    "portuguese",
    "romanian",
    "russian",
    "spanish",
    "swedish",
    "tamil",
    "turkish",
]

# Data types that can be indexed
DataType = Literal["string", "json", "hash"]


# Schema definitions
class DetailedField(TypedDict, total=False):
    """Detailed field configuration with options."""

    type: FieldType
    fast: bool
    noTokenize: bool
    noStem: bool
    from_: str  # Using from_ to avoid Python keyword


# Schema can be a nested structure of fields
NestedIndexSchema = Dict[str, Union[FieldType, DetailedField, "NestedIndexSchema"]]
FlatIndexSchema = Dict[str, Union[FieldType, DetailedField]]

RootQueryFilter = Dict[str, Any]

# Query options
class HighlightOptions(TypedDict, total=False):
    """Highlighting options for query results."""

    fields: List[str]
    tags: Dict[str, str]  # "open" and "close" tags


class QueryOptions(TypedDict, total=False):
    """Options for querying an index."""

    filter: Any
    limit: int
    offset: int
    orderBy: Dict[str, Literal["ASC", "DESC"]]
    select: Dict[str, bool]  # Field name to include/exclude
    highlight: HighlightOptions


# Query result
class QueryResult(TypedDict, total=False):
    """Result from a query operation."""

    key: str
    score: str
    data: Dict[str, Any]


# Index description
class DescribeFieldInfo(TypedDict, total=False):
    """Field information from describe command."""

    type: FieldType
    fast: bool
    noTokenize: bool
    noStem: bool


class IndexDescription(TypedDict, total=False):
    """Description of an index."""

    name: str
    dataType: DataType
    prefixes: List[str]
    language: Language
    schema: Dict[str, DescribeFieldInfo]


# Create index parameters
class CreateIndexParams(TypedDict, total=False):
    """Parameters for creating a new index."""

    name: str
    prefix: Union[str, List[str]]
    dataType: DataType
    schema: Union[NestedIndexSchema, FlatIndexSchema]
    language: Language
    skipInitialScan: bool
    existsOk: bool
