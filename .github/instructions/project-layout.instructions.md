---
applyTo: '**'
---
# Adding New Redis Commands to Upstash Redis Python Package

## Package Structure Overview

The Upstash Redis Python package is organized as follows:

```
upstash_redis/
├── __init__.py          # Package exports
├── client.py            # Synchronous Redis client
├── commands.py          # Command implementations
├── commands.pyi         # Type hints for commands
├── errors.py            # Custom exceptions
├── format.py            # Response formatters
├── http.py              # HTTP client for Redis REST API
├── typing.py            # Type definitions
├── utils.py             # Utility functions
└── asyncio/
    ├── __init__.py
    └── client.py        # Asynchronous Redis client

tests/
├── commands/            # Command-specific tests organized by category
│   ├── hash/           # Hash command tests
│   ├── json/           # JSON command tests
│   ├── list/           # List command tests
│   ├── set/            # Set command tests
│   ├── sortedSet/      # Sorted set command tests
│   ├── string/         # String command tests
│   └── asyncio/        # Async versions of tests
```

## Steps to Add a New Redis Command

### 1. Implement the Command in `commands.py`

Add your command method to the `Commands` class:

```python
# filepath: upstash_redis/commands.py
def your_new_command(self, key: str, *args, **kwargs) -> CommandsProtocol:
    """
    Description of your command.
    
    Args:
        key: The Redis key
        *args: Additional arguments
        **kwargs: Additional keyword arguments
    
    Returns:
        CommandsProtocol: Command object for execution
    """
    return self._execute_command("YOUR_REDIS_COMMAND", key, *args, **kwargs)
```

### 2. Add Type Hints in `commands.pyi`

Update the type stub file with your command signature:

```python
# filepath: upstash_redis/commands.pyi
def your_new_command(self, key: str, *args, **kwargs) -> CommandsProtocol: ...
```

### 3. Update Client Classes (if needed)

If your command requires special handling, update both sync and async clients:

```python
# filepath: upstash_redis/client.py
# Add any client-specific logic if needed

# filepath: upstash_redis/asyncio/client.py  
# Add async version if special handling is needed
```

### 4. Add Response Formatting (if needed)

If your command returns data that needs special formatting, add a formatter in `format.py`:

```python
# filepath: upstash_redis/format.py
def format_your_command_response(response: Any) -> YourReturnType:
    """Format the response from your Redis command."""
    # Implementation here
    pass
```

### 5. Write Comprehensive Tests

Create test files in the appropriate category folder:

```python
# filepath: tests/commands/{category}/test_your_new_command.py
import pytest
from upstash_redis import Redis

def test_your_new_command_basic():
    """Test basic functionality of your new command."""
    redis = Redis.from_env()
    result = redis.your_new_command("test_key", "arg1", "arg2")
    # Add assertions

def test_your_new_command_edge_cases():
    """Test edge cases and error conditions."""
    # Add edge case tests

# If async support is needed:
# filepath: tests/commands/asyncio/test_your_new_command.py
import pytest
from upstash_redis.asyncio import Redis as AsyncRedis

@pytest.mark.asyncio
async def test_your_new_command_async():
    """Test async version of your new command."""
    redis = AsyncRedis.from_env()
    result = await redis.your_new_command("test_key", "arg1", "arg2")
    # Add assertions
```

### 6. Update Package Exports (if needed)

If you're adding a new public class or function, update `__init__.py`:

```python
# filepath: upstash_redis/__init__.py
from upstash_redis.your_new_module import YourNewClass

__all__ = ["AsyncRedis", "Redis", "YourNewClass"]
```

## Command Categories and Organization

Commands are typically organized into these categories:

- **String**: Basic key-value operations (`GET`, `SET`, etc.)
- **Hash**: Hash field operations (`HGET`, `HSET`, etc.)
- **List**: List operations (`LPUSH`, `RPOP`, etc.)
- **Set**: Set operations (`SADD`, `SREM`, etc.)
- **Sorted Set**: Sorted set operations (`ZADD`, `ZREM`, etc.)
- **JSON**: JSON operations (`JSON.GET`, `JSON.SET`, etc.)
- **Generic**: Key management (`DEL`, `EXISTS`, etc.)
- **Server**: Server management commands

## Testing Guidelines

1. **Test file naming**: `test_{command_name}.py`
2. **Test function naming**: `test_{command_name}_{scenario}`
3. **Include both positive and negative test cases**
4. **Test with different data types and edge cases**
5. **Add async tests if the command supports async operations**
6. **Use appropriate fixtures from `conftest.py`**

## Example: Adding a New Hash Command

Here's a complete example of adding a hypothetical `HMERGE` command:

```python
# filepath: upstash_redis/commands.py
def hmerge(self, key: str, source_key: str) -> CommandsProtocol:
    """
    Merge hash from source_key into key.
    
    Args:
        key: Destination hash key
        source_key: Source hash key to merge from
    
    Returns:
        CommandsProtocol: Command for execution
    """
    return self._execute_command("HMERGE", key, source_key)
```

```python
# filepath: tests/commands/hash/test_hmerge.py
import pytest
from upstash_redis import Redis

def test_hmerge_basic(redis_client):
    """Test basic HMERGE functionality."""
    redis = redis_client
    
    # Setup
    redis.hset("hash1", {"field1": "value1", "field2": "value2"})
    redis.hset("hash2", {"field3": "value3", "field2": "overwrite"})
    
    # Execute
    result = redis.hmerge("hash1", "hash2")
    
    # Verify
    merged_hash = redis.hgetall("hash1")
    assert merged_hash["field1"] == "value1"
    assert merged_hash["field2"] == "overwrite"  # Should be overwritten
    assert merged_hash["field3"] == "value3"     # Should be added
```

## Running Tests

To run tests for your new command:

```bash
# Run specific test file
pytest tests/commands/hash/test_your_command.py -v

# Run all tests in a category
pytest tests/commands/hash/ -v

# Run all tests
pytest tests/ -v
```

Follow this structure and you'll have a well-integrated Redis command that follows the package's conventions and patterns.