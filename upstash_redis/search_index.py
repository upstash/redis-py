"""Search index functionality for Upstash Redis."""

from typing import Any, Dict, List, Optional, Union

from upstash_redis.search_types import (
    CreateIndexParams,
    IndexDescription,
    QueryOptions,
    QueryResult,
    NestedIndexSchema,
    FlatIndexSchema,
    RootQueryFilter,
)
from upstash_redis.search_utils import (
    build_create_index_command,
    build_query_command,
    deserialize_describe_response,
    deserialize_query_response,
    parse_count_response,
)


class SearchIndex:
    """
    Represents a search index in Upstash Redis.

    This class provides methods to interact with a search index including
    querying, counting, describing, and dropping the index.
    """

    def __init__(
        self,
        name: str,
        client: Any,
        schema: Optional[Union[NestedIndexSchema, FlatIndexSchema]] = None,
    ):
        """
        Initialize a SearchIndex instance.

        Args:
            name: Name of the index
            client: Redis client instance (sync or async)
            schema: Optional schema definition for the index
        """
        self.name = name
        self.schema = schema
        self._client = client

    def wait_indexing(self) -> Any:
        """
        Wait for the index to finish indexing all existing keys.

        Returns:
            The result of the command execution

        Example:
        ```python
        index.wait_indexing()
        ```
        """
        command = ["SEARCH.WAITINDEXING", self.name]
        return self._client.execute(command)

    def describe(self) -> IndexDescription:
        """
        Get detailed information about the index structure.

        Returns:
            IndexDescription with schema, prefixes, and other metadata

        Example:
        ```python
        description = index.describe()
        print(description["schema"])
        ```
        """
        command = ["SEARCH.DESCRIBE", self.name]
        raw_result = self._client.execute(command)

        # If the client returns a coroutine (async), we can't deserialize here
        # The async version will need to override this method
        if hasattr(raw_result, "__await__"):
            # For async client, return the coroutine wrapped in deserializer
            async def _async_describe():
                result = await raw_result
                return deserialize_describe_response(result)

            return _async_describe()

        return deserialize_describe_response(raw_result)

    def query(
        self,
        *,
        filter: RootQueryFilter,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderBy: Optional[Dict[str, str]] = None,
        select: Optional[Dict[str, bool]] = None,
        highlight: Optional[Dict[str, Any]] = None,
    ) -> List[QueryResult]:
        """
        Query the index with filters and options.

        Args:
            filter: Filter specification mapping field names to conditions
            limit: Maximum number of results to return
            offset: Number of results to skip (for pagination)
            orderBy: Field to sort by with direction ("ASC" or "DESC")
            select: Fields to include/exclude in results
            highlight: Highlighting options for matched terms

        Returns:
            List of query results with keys, scores, and data

        Example:
        ```python
        # Simple text search
        results = index.query(filter={"name": {"$eq": "Laptop"}})

        # With pagination and sorting
        results = index.query(
            filter={"category": {"$eq": "electronics"}},
            limit=10,
            offset=0,
            orderBy={"price": "ASC"}
        )
        ```
        """
        options: QueryOptions = {"filter": filter}
        if limit is not None:
            options["limit"] = limit
        if offset is not None:
            options["offset"] = offset
        if orderBy is not None:
            options["orderBy"] = orderBy
        if select is not None:
            options["select"] = select
        if highlight is not None:
            options["highlight"] = highlight
        
        command = build_query_command("SEARCH.QUERY", self.name, options)
        raw_result = self._client.execute(command)

        # Handle async
        if hasattr(raw_result, "__await__"):

            async def _async_query():
                result = await raw_result
                return deserialize_query_response(result)

            return _async_query()

        return deserialize_query_response(raw_result)

    def count(self, filter: RootQueryFilter) -> Dict[str, int]:
        """
        Count documents matching a filter.

        Args:
            filter: Filter specification mapping field names to conditions

        Returns:
            Dictionary with "count" key containing the number of matches

        Example:
        ```python
        result = index.count({"active": {"$eq": True}})
        print(result["count"])  # e.g., 42
        ```
        """
        command = build_query_command("SEARCH.COUNT", self.name, {"filter": filter})
        raw_result = self._client.execute(command)

        # Handle async
        if hasattr(raw_result, "__await__"):

            async def _async_count():
                result = await raw_result
                return {"count": parse_count_response(result)}

            return _async_count()

        return {"count": parse_count_response(raw_result)}

    def drop(self) -> Union[int, Any]:
        """
        Drop (delete) the index.

        Returns:
            1 if the index was dropped, 0 if it didn't exist

        Example:
        ```python
        result = index.drop()
        print(result)  # 1
        ```
        """
        command = ["SEARCH.DROP", self.name]
        return self._client.execute(command)


def _create_index(client: Any, params: CreateIndexParams) -> SearchIndex:
    """
    Create a new search index.

    Args:
        client: Redis client instance (sync or async)
        params: Index creation parameters including name, schema, data type, etc.

    Returns:
        SearchIndex instance for the newly created index

    Example:
    ```python
    schema = {
        "name": "TEXT",
        "age": {"type": "U64", "fast": True},
        "active": "BOOL"
    }

    index = create_index(redis, {
        "name": "users-idx",
        "prefix": "user:",
        "dataType": "json",
        "schema": schema
    })
    ```
    """
    name = params["name"]
    schema = params.get("schema")

    # Build and execute create command
    command = build_create_index_command(params)
    result = client.execute(command)

    # Handle async
    if hasattr(result, "__await__"):

        async def _async_create():
            await result
            return SearchIndex(name=name, client=client, schema=schema)

        return _async_create()

    return SearchIndex(name=name, client=client, schema=schema)


def _init_index(
    client: Any,
    name: str,
    schema: Optional[Union[NestedIndexSchema, FlatIndexSchema]] = None,
) -> SearchIndex:
    """
    Initialize a SearchIndex instance for an existing index.

    This function does not create the index, it only creates a Python object
    to interact with an existing index.

    Args:
        client: Redis client instance (sync or async)
        name: Name of the existing index
        schema: Optional schema definition for better type hints

    Returns:
        SearchIndex instance

    Example:
    ```python
    # Without schema
    index = init_index(redis, "users-idx")

    # With schema for better typing
    schema = {
        "name": "TEXT",
        "age": {"type": "U64", "fast": True}
    }
    index = init_index(redis, "users-idx", schema)
    ```
    """
    return SearchIndex(name=name, client=client, schema=schema)
