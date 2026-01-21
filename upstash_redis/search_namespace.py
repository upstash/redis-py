"""Search namespace for Redis client."""

from typing import Any, Optional, Union

from upstash_redis.search_index import SearchIndex, _create_index, _init_index
from upstash_redis.search_types import CreateIndexParams, NestedIndexSchema, FlatIndexSchema


class SearchNamespace:
    """
    Namespace for search index operations.
    
    Access via redis.search.create_index() or redis.search.index()
    """
    
    def __init__(self, client: Any):
        """
        Initialize the search namespace.
        
        Args:
            client: Redis client instance (sync or async)
        """
        self._client = client
    
    def create_index(
        self,
        *,
        name: str,
        schema: Union[NestedIndexSchema, FlatIndexSchema],
        dataType: str,
        prefix: Union[str, list[str]],
        language: Optional[str] = None,
        skipInitialScan: bool = False,
        existsOk: bool = False,
    ) -> SearchIndex:
        """
        Create a new search index.
        
        Args:
            name: Name of the index
            schema: Schema definition mapping field names to types
            dataType: Type of data being indexed ("string", "json", or "hash")
            prefix: Key prefix(es) for automatic indexing
            language: Optional language for text analysis
            skipInitialScan: Skip indexing existing keys
            existsOk: Don't error if index already exists
        
        Returns:
            SearchIndex instance for the newly created index
        
        Example:
        ```python
        schema = {
            "name": "TEXT",
            "age": {"type": "U64", "fast": True},
            "active": "BOOL"
        }
        
        index = redis.search.create_index(
            name="users-idx",
            prefix="user:",
            dataType="json",
            schema=schema
        )
        ```
        """
        params: CreateIndexParams = {
            "name": name,
            "schema": schema,
            "dataType": dataType,
            "prefix": prefix,
        }
        if language is not None:
            params["language"] = language
        if skipInitialScan:
            params["skipInitialScan"] = skipInitialScan
        if existsOk:
            params["existsOk"] = existsOk
        
        return _create_index(self._client, params)
    
    def index(
        self,
        name: str,
        schema: Optional[Union[NestedIndexSchema, FlatIndexSchema]] = None
    ) -> SearchIndex:
        """
        Initialize a SearchIndex instance for an existing index.
        
        This method does not create the index, it only creates a Python object
        to interact with an existing index.
        
        Args:
            name: Name of the existing index
            schema: Optional schema definition for better type hints
        
        Returns:
            SearchIndex instance
        
        Example:
        ```python
        # Without schema
        index = redis.search.index("users-idx")
        
        # With schema for better typing
        schema = {
            "name": "TEXT",
            "age": {"type": "U64", "fast": True}
        }
        index = redis.search.index("users-idx", schema)
        ```
        """
        return _init_index(self._client, name, schema)
