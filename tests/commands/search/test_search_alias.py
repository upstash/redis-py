"""Tests for search alias functionality."""

import pytest

from upstash_redis import Redis


def random_id() -> str:
    import random
    import string

    return "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


@pytest.fixture
def redis_client() -> Redis:
    return Redis.from_env()


@pytest.fixture
def cleanup_indexes():
    indexes = []
    yield indexes
    redis = Redis.from_env()
    for index_name in indexes:
        try:
            redis.search.index(index_name).drop()
        except Exception:
            pass


class TestAlias:
    """Tests for alias commands."""

    def test_add_alias_via_index(self, redis_client: Redis, cleanup_indexes):
        """Test adding an alias via the index method."""
        name = f"test-alias-idx-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.append(name)

        index = redis_client.search.create_index(
            name=name, schema={"name": "TEXT"}, data_type="string", prefixes=f"{name}:"
        )

        result = index.add_alias(alias=alias_name)
        assert result == 1

        # Clean up alias
        redis_client.search.alias.delete(alias=alias_name)

    def test_alias_add(self, redis_client: Redis, cleanup_indexes):
        """Test adding an alias via search.alias.add."""
        name = f"test-alias-add-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.append(name)

        redis_client.search.create_index(
            name=name, schema={"name": "TEXT"}, data_type="string", prefixes=f"{name}:"
        )

        result = redis_client.search.alias.add(index_name=name, alias=alias_name)
        assert result == 1

        # Clean up alias
        redis_client.search.alias.delete(alias=alias_name)

    def test_alias_delete(self, redis_client: Redis, cleanup_indexes):
        """Test deleting an alias."""
        name = f"test-alias-del-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.append(name)

        redis_client.search.create_index(
            name=name, schema={"name": "TEXT"}, data_type="string", prefixes=f"{name}:"
        )

        redis_client.search.alias.add(index_name=name, alias=alias_name)

        result = redis_client.search.alias.delete(alias=alias_name)
        assert result == 1

    def test_alias_list(self, redis_client: Redis, cleanup_indexes):
        """Test listing aliases."""
        name = f"test-alias-list-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.append(name)

        redis_client.search.create_index(
            name=name, schema={"name": "TEXT"}, data_type="string", prefixes=f"{name}:"
        )

        redis_client.search.alias.add(index_name=name, alias=alias_name)

        aliases = redis_client.search.alias.list()
        assert isinstance(aliases, dict)
        assert alias_name in aliases
        assert aliases[alias_name] == name

        # Clean up alias
        redis_client.search.alias.delete(alias=alias_name)

    def test_update_alias_to_new_index(self, redis_client: Redis, cleanup_indexes):
        """Test updating an existing alias to point to a new index."""
        name1 = f"test-alias-upd1-{random_id()}"
        name2 = f"test-alias-upd2-{random_id()}"
        alias_name = f"alias-{random_id()}"
        cleanup_indexes.extend([name1, name2])

        redis_client.search.create_index(
            name=name1,
            schema={"name": "TEXT"},
            data_type="string",
            prefixes=f"{name1}:",
        )
        redis_client.search.create_index(
            name=name2,
            schema={"name": "TEXT"},
            data_type="string",
            prefixes=f"{name2}:",
        )

        # Add alias pointing to first index
        result1 = redis_client.search.alias.add(index_name=name1, alias=alias_name)
        assert result1 == 1

        # Update alias to point to second index
        result2 = redis_client.search.alias.add(index_name=name2, alias=alias_name)
        assert result2 == 2

        aliases = redis_client.search.alias.list()
        assert aliases[alias_name] == name2

        # Clean up alias
        redis_client.search.alias.delete(alias=alias_name)
