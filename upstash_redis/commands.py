import datetime
from typing import Any, Awaitable, Dict, List, Literal, Mapping, Optional, Tuple, Union

from upstash_redis.typing import FloatMinMaxT, ValueT, JSONValueT
from upstash_redis.utils import (
    handle_georadius_write_exceptions,
    handle_geosearch_exceptions,
    handle_non_deprecated_zrange_exceptions,
    handle_zrangebylex_exceptions,
    number_are_not_none,
)

ResponseT = Union[Awaitable, Any]


class Commands:
    def execute(self, command: List) -> ResponseT:
        raise NotImplementedError("execute")

    def bitcount(
        self, key: str, start: Optional[int] = None, end: Optional[int] = None
    ) -> ResponseT:
        """
        Returns the number of bits set to 1 in a given range.

        Example:
        ```python
        redis.setbit("mykey", 7, 1)
        redis.setbit("mykey", 8, 1)
        redis.setbit("mykey", 9, 1)

        assert redis.bitcount("mykey", 0, 10) == 3
        ```

        See https://redis.io/commands/bitcount
        """

        if number_are_not_none(start, end, number=1):
            raise Exception('Both "start" and "end" must be specified.')

        command: List = ["BITCOUNT", key]

        if start is not None:
            command.extend([start, end])

        return self.execute(command)

    def bitfield(self, key: str) -> "BitFieldCommands":
        """
        Returns a BitFieldCommands instance that can be used to execute BITFIELD commands on one key.

        Example:
        ```python
        redis.set("mykey", 0)
        result = redis.bitfield("mykey")
                .set("u4", 0, 16)
                .incr("u4", 4, 1)
                .execute()

        assert result == [0, 1]
        ```

        See https://redis.io/commands/bitfield
        """

        return BitFieldCommands(key=key, client=self)

    def bitfield_ro(self, key: str) -> "BitFieldROCommands":
        """
        Returns a BitFieldROCommands instance that can be used to execute BITFIELD_RO commands on one key.

        Example:
        ```python
        redis.set("mykey", 0)
        result = redis.bitfield_ro("mykey")
                .get("u4", 0)
                .execute()

        assert result == [0]
        ```

        See https://redis.io/commands/bitfield_ro
        """

        return BitFieldROCommands(key=key, client=self)

    def bitop(
        self, operation: Literal["AND", "OR", "XOR", "NOT"], destkey: str, *keys: str
    ) -> ResponseT:
        """
        Performs a bitwise operation between multiple keys (containing string values) and stores the result in the
        destination key.

        Example:
        ```python
        redis.setbit("key1", 0, 1)
        redis.setbit("key2", 0, 0)
        redis.setbit("key2", 1, 1)

        assert redis.bitop("AND", "dest", "key1", "key2") == 1
        assert redis.getbit("dest", 0) == 0
        assert redis.getbit("dest", 1) == 0
        ```

        See https://redis.io/commands/bitop
        """

        if len(keys) == 0:
            raise Exception("At least one source key must be specified.")

        if operation == "NOT" and len(keys) > 1:
            raise Exception(
                'The "NOT " operation takes only one source key as argument.'
            )

        command: List = ["BITOP", operation, destkey, *keys]

        return self.execute(command)

    def bitpos(
        self,
        key: str,
        bit: Literal[0, 1],
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> ResponseT:
        """
        Returns the position of the first bit set to 1 or 0 in a string.
        If no bit is set, -1 is returned.

        Example:
        ```python
        redis.setbit("mykey", 7, 1)
        redis.setbit("mykey", 8, 1)

        assert redis.bitpos("mykey", 1) == 7
        assert redis.bitpos("mykey", 0) == 0

        # With a range
        assert redis.bitpos("mykey", 1, 0, 2) == 0
        assert redis.bitpos("mykey", 1, 2, 3) == -1
        ```

        See https://redis.io/commands/bitpos
        """

        if start is None and end is not None:
            raise Exception('"end" is specified, but "start" is missing.')

        command: List = ["BITPOS", key, bit]

        if start is not None:
            command.append(start)

        if end is not None:
            command.append(end)

        return self.execute(command)

    def getbit(self, key: str, offset: int) -> ResponseT:
        """
        Returns the bit value at offset in the string value stored at key.

        Example:
        ```python
        redis.setbit("mykey", 7, 1)

        assert redis.getbit("mykey", 7) == 1
        ```

        See https://redis.io/commands/getbit
        """

        command: List = ["GETBIT", key, offset]

        return self.execute(command)

    def setbit(self, key: str, offset: int, value: Literal[0, 1]) -> ResponseT:
        """
        Sets or clears the bit at offset in the string value stored at key.
        If the offset is larger than the current length of the string,
        the string is padded with zero-bytes to make offset fit.

        Returns the original bit value stored at offset.

        Example:
        ```python
        redis.setbit("mykey", 7, 1)

        assert redis.getbit("mykey", 7) == 1
        ```

        See https://redis.io/commands/setbit
        """

        command: List = ["SETBIT", key, offset, value]

        return self.execute(command)

    def ping(self, message: Optional[str] = None) -> ResponseT:
        """
        Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk.

        Example:
        ```python
        assert redis.ping() == "PONG"
        assert redis.ping("Hello") == "Hello"
        ```

        See https://redis.io/commands/ping
        """

        command: List = ["PING"]

        if message is not None:
            command.append(message)

        return self.execute(command)

    def echo(self, message: str) -> ResponseT:
        """
        Returns the message.

        Example:
        ```python
        assert redis.echo("Hello") == "Hello"
        ```

        See https://redis.io/commands/echo
        """

        command: List = ["ECHO", message]

        return self.execute(command)

    def copy(self, source: str, destination: str, replace: bool = False) -> ResponseT:
        """
        Copies the value stored at the source key to the destination key.
        By default, the destination key is created only when the source key exists.

        :param replace: if True, the destination key is deleted before copying the value.

        Example:
        ```python
        redis.set("mykey", "Hello")
        redis.copy("mykey", "myotherkey")

        assert redis.get("myotherkey") == "Hello"
        ```

        See https://redis.io/commands/copy
        """

        command: List = ["COPY", source, destination]

        if replace:
            command.append("REPLACE")

        return self.execute(command)

    def delete(self, *keys: str) -> ResponseT:
        """
        Deletes one or more keys.
        Returns the number of keys that were removed.

        Example:
        ```python
        redis.set("key1", "Hello")
        redis.set("key2", "World")
        redis.delete("key1", "key2")

        assert redis.get("key1") is None
        assert redis.get("key2") is None
        ```

        See https://redis.io/commands/del
        """

        if len(keys) == 0:
            raise Exception("At least one key must be deleted.")

        command: List = ["DEL", *keys]

        return self.execute(command)

    def exists(self, *keys: str) -> ResponseT:
        """
        Returns the number of keys existing among the ones specified as arguments.

        Example:
        ```python
        redis.set("key1", "Hello")
        redis.set("key2", "World")

        assert redis.exists("key1", "key2") == 2

        redis.delete("key1")

        assert redis.exists("key1", "key2") == 1
        ```

        See https://redis.io/commands/exists
        """

        if len(keys) == 0:
            raise Exception("At least one key must be checked.")

        command: List = ["EXISTS", *keys]

        return self.execute(command)

    def expire(
        self,
        key: str,
        seconds: Union[int, datetime.timedelta],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> ResponseT:
        """
        Sets a timeout on a key in seconds.
        After the timeout has expired, the key will automatically be deleted.

        :param seconds: the timeout in seconds as int or datetime.timedelta object
        :param nx: Set expiry only when the key has no expiry
        :param xx: Set expiry only when the key has an existing expiry
        :param gt: Set expiry only when the new expiry is greater than current one
        :param lt: Set expiry only when the new expiry is less than current one

        Example
        ```python
        # With seconds
        redis.set("mykey", "Hello")
        redis.expire("mykey", 5)

        assert redis.get("mykey") == "Hello"

        time.sleep(5)

        assert redis.get("mykey") is None

        # With a timedelta
        redis.set("mykey", "Hello")
        redis.expire("mykey", datetime.timedelta(seconds=5))
        ```

        See https://redis.io/commands/expire
        """

        if isinstance(seconds, datetime.timedelta):
            seconds = int(seconds.total_seconds())

        command: List = ["EXPIRE", key, seconds]

        if nx:
            command.append("NX")
        if xx:
            command.append("XX")
        if gt:
            command.append("GT")
        if lt:
            command.append("LT")

        return self.execute(command)

    def hexpire(
        self,
        key: str,
        fields: Union[str, List[str]],
        seconds: Union[int, datetime.timedelta],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> ResponseT:
        """
        Sets a timeout on a hash field in seconds.
        After the timeout has expired, the hash field will automatically be deleted.

        :param key: The key of the hash.
        :param field: The field within the hash to set the expiry for.
        :param seconds: The timeout in seconds as an int or a datetime.timedelta object.
        :param nx: Set expiry only when the field has no expiry.
        :param xx: Set expiry only when the field has an existing expiry.
        :param gt: Set expiry only when the new expiry is greater than the current one.
        :param lt: Set expiry only when the new expiry is less than the current one.

        Example:
        ```python
        # With seconds
        redis.hset("myhash", "field1", "value1")
        redis.hexpire("myhash", "field1", 5)

        assert redis.hget("myhash", "field1") == "value1"

        time.sleep(5)

        assert redis.hget("myhash", "field1") is None

        # With a timedelta
        redis.hset("myhash", "field1", "value1")
        redis.hexpire("myhash", "field1", datetime.timedelta(seconds=5))
        ```

        See https://redis.io/commands/hexpire for more details on expiration behavior.
        """

        if isinstance(seconds, datetime.timedelta):
            seconds = int(seconds.total_seconds())

        command: List = ["HEXPIRE", key, seconds]

        if nx:
            command.append("NX")
        if xx:
            command.append("XX")
        if gt:
            command.append("GT")
        if lt:
            command.append("LT")

        if isinstance(fields, str):
            fields = [fields]

        command.extend(["FIELDS", len(fields), *fields])

        return self.execute(command)

    def hpexpire(
        self,
        key: str,
        fields: Union[str, List[str]],
        milliseconds: Union[int, datetime.timedelta],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> ResponseT:
        """
        Sets a timeout on a hash field in milliseconds.
        After the timeout has expired, the hash field will automatically be deleted.

        :param key: The key of the hash.
        :param fields: The field(s) within the hash to set the expiry for.
        :param milliseconds: The timeout in milliseconds as an int or a datetime.timedelta object.
        :param nx: Set expiry only when the field has no expiry.
        :param xx: Set expiry only when the field has an existing expiry.
        :param gt: Set expiry only when the new expiry is greater than the current one.
        :param lt: Set expiry only when the new expiry is less than the current one.

        See https://redis.io/commands/hexpire
        """
        if isinstance(milliseconds, datetime.timedelta):
            milliseconds = int(milliseconds.total_seconds() * 1000)

        command: List = ["HPEXPIRE", key, milliseconds]

        if nx:
            command.append("NX")
        if xx:
            command.append("XX")
        if gt:
            command.append("GT")
        if lt:
            command.append("LT")

        if isinstance(fields, str):
            fields = [fields]

        command.extend(["FIELDS", len(fields), *fields])

        return self.execute(command)

    def hexpireat(
        self,
        key: str,
        fields: Union[str, List[str]],
        unix_time_seconds: Union[int, datetime.datetime],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> ResponseT:
        """
        Set an expiration time for specific fields in a hash at a given Unix timestamp.

        :param key: The key of the hash.
        :param fields: The field or list of fields in the hash to set the expiration for.
        :param unix_time_seconds: the timeout in seconds as int or datetime.timedelta object
        :param nx: Set expiry only when the key has no expiry
        :param xx: Set expiry only when the key has an existing expiry
        :param gt: Set expiry only when the new expiry is greater than current one
        :param lt: Set expiry only when the new expiry is less than current one

        Example:
        ```python
        redis.hset("myhash", "field1", "value1")
        redis.hexpireat("my_hash", ["field1", "field2"], 1672531200)
        ```

        See https://redis.io/commands/hexpireat
        """
        if isinstance(unix_time_seconds, datetime.datetime):
            unix_time_seconds = int(unix_time_seconds.timestamp())

        command: List = ["HEXPIREAT", key, unix_time_seconds]

        if nx:
            command.append("NX")
        if xx:
            command.append("XX")
        if gt:
            command.append("GT")
        if lt:
            command.append("LT")

        if isinstance(fields, str):
            fields = [fields]

        command.extend(["FIELDS", len(fields), *fields])

        return self.execute(command)

    def hpexpireat(
        self,
        key: str,
        fields: Union[str, List[str]],
        unix_time_milliseconds: Union[int, datetime.datetime],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> ResponseT:
        """
        Set an expiration time for specific fields in a hash at a given Unix timestamp in milliseconds.

        :param key: The key of the hash.
        :param fields: The field or list of fields in the hash to set the expiration for.
        :param unix_time_milliseconds: the timeout milliseconds as int or datetime.timedelta object
        :param nx: Set expiry only when the key has no expiry
        :param xx: Set expiry only when the key has an existing expiry
        :param gt: Set expiry only when the new expiry is greater than current one
        :param lt: Set expiry only when the new expiry is less than current one

        Example:
        ```python
        redis.hset("myhash", "field1", "value1")
        redis.hpexpireat("my_hash", ["field1", "field2"], 1672531200000)
        ```

        See https://redis.io/commands/hpexpireat
        """
        if isinstance(unix_time_milliseconds, datetime.datetime):
            unix_time_milliseconds = int(unix_time_milliseconds.timestamp() * 1000)

        command: List = ["HPEXPIREAT", key, unix_time_milliseconds]

        if nx:
            command.append("NX")
        if xx:
            command.append("XX")
        if gt:
            command.append("GT")
        if lt:
            command.append("LT")

        if isinstance(fields, str):
            fields = [fields]

        command.extend(["FIELDS", len(fields), *fields])

        return self.execute(command)

    def httl(self, key: str, fields: Union[str, List[str]]) -> ResponseT:
        """
        Retrieve the time-to-live (TTL) of one or more fields within a hash in seconds.

        :param key: The key of the hash.
        :param fields: One or more field names within the hash to check the TTL for.

        Example:
        ```python
        redis.hset("myhash", "field1", "value1")
        redis.hpexpireat("my_hash", ["field1", "field2"], 1672531200000)
        redis.httl("myhash", "field1")
        ```

        See https://redis.io/commands/httl
        """

        if isinstance(fields, str):
            fields = [fields]

        command: List = ["HTTL", key, "FIELDS", len(fields), *fields]
        return self.execute(command)

    def hpttl(self, key: str, fields: Union[str, List[str]]) -> ResponseT:
        """
        Retrieve the time-to-live (TTL) of one or more fields within a hash in milliseconds.

        :param key: The key of the hash.
        :param fields: One or more field names within the hash to check the TTL for.

        Example:
        ```python
        redis.hset("myhash", "field1", "value1")
        redis.hpexpireat("my_hash", ["field1", "field2"], 1672531200000)
        redis.hpttl("myhash", "field1")
        ```

        See https://redis.io/commands/hpttl
        """

        if isinstance(fields, str):
            fields = [fields]

        command: List = ["HPTTL", key, "FIELDS", len(fields), *fields]
        return self.execute(command)

    def hexpiretime(self, key: str, fields: Union[str, List[str]]) -> ResponseT:
        """
        Retrieve the expiration time (as unix timestamp seconds) of one or more
        hash fields in a Redis hash.

        :param key: The key of the Redis hash.
        :param fields: One or more field names within the hash.

        Example:
        ```python
        redis.hset("myhash", "field1", "value1")
        redis.hpexpireat("my_hash", ["field1", "field2"], 1672531200000)
        redis.hexpiretime("myhash", "field1")
        ```

        See https://redis.io/commands/hexpiretime
        """

        if isinstance(fields, str):
            fields = [fields]

        command: List = ["HEXPIRETIME", key, "FIELDS", len(fields), *fields]
        return self.execute(command)

    def hpexpiretime(self, key: str, fields: Union[str, List[str]]) -> ResponseT:
        """
        Retrieve the expiration time (as unix timestamp in milliseconds) of one or
        more hash fields in a Redis hash.

        :param key: The key of the Redis hash.
        :param fields: One or more field names within the hash.

        Example:
        ```python
        redis.hset("myhash", "field1", "value1")
        redis.hpexpireat("my_hash", ["field1", "field2"], 1672531200000)
        redis.hpexpiretime("myhash", "field1")
        ```

        See https://redis.io/commands/hpexpiretime
        """

        if isinstance(fields, str):
            fields = [fields]

        command: List = ["HPEXPIRETIME", key, "FIELDS", len(fields), *fields]
        return self.execute(command)

    def hpersist(self, key: str, fields: Union[str, List[str]]) -> ResponseT:
        """
        Removes the expiration from one or more fields in a hash.

        :param key: The key of the Redis hash.
        :param fields: One or more field names within the hash to remove expiration from.

        Example:
        ```python
        redis.hset("myhash", "field1", "value1")
        redis.hpexpireat("my_hash", ["field1", "field2"], 1672531200000)
        redis.hpersist("myhash", "field1")
        ```

        See https://redis.io/commands/hpersist
        """

        if isinstance(fields, str):
            fields = [fields]

        command: List = ["HPERSIST", key, "FIELDS", len(fields), *fields]
        return self.execute(command)

    def expireat(
        self,
        key: str,
        unix_time_seconds: Union[int, datetime.datetime],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> ResponseT:
        """
        Expires a key at a specific unix timestamp (seconds).
        After the timeout has expired, the key will automatically be deleted.

        :param seconds: the timeout in unix seconds timestamp as int or a datetime.datetime object.
        :param nx: Set expiry only when the key has no expiry
        :param xx: Set expiry only when the key has an existing expiry
        :param gt: Set expiry only when the new expiry is greater than current one
        :param lt: Set expiry only when the new expiry is less than current one

        Example
        ```python
        # With a datetime object
        redis.set("mykey", "Hello")
        redis.expireat("mykey", datetime.datetime.now() + datetime.timedelta(seconds=5))

        # With a unix timestamp
        redis.set("mykey", "Hello")
        redis.expireat("mykey", int(time.time()) + 5)
        ```

        See https://redis.io/commands/expireat
        """

        if isinstance(unix_time_seconds, datetime.datetime):
            unix_time_seconds = int(unix_time_seconds.timestamp())

        command: List = ["EXPIREAT", key, unix_time_seconds]

        if nx:
            command.append("NX")
        if xx:
            command.append("XX")
        if gt:
            command.append("GT")
        if lt:
            command.append("LT")

        return self.execute(command)

    def keys(self, pattern: str) -> ResponseT:
        """
        Returns all keys matching pattern.

        Example:
        ```python
        redis.set("key1", "Hello")
        redis.set("key2", "World")

        assert redis.keys("key*") == ["key1", "key2"]
        ```

        See https://redis.io/commands/keys
        """

        command: List = ["KEYS", pattern]

        return self.execute(command)

    def persist(self, key: str) -> ResponseT:
        """
        Removes the expiration from a key.

        Returns True if the timeout was removed,
        False if key does not exist or does not have an associated timeout.

        Example:
        ```python
        redis.set("key1", "Hello")
        redis.expire("key1", 10)

        assert redis.ttl("key1") == 10

        redis.persist("key1")

        assert redis.ttl("key1") == -1
        ```

        See https://redis.io/commands/persist
        """

        command: List = ["PERSIST", key]

        return self.execute(command)

    def pexpire(
        self,
        key: str,
        milliseconds: Union[int, datetime.timedelta],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> ResponseT:
        """
        Sets a timeout on a key in milliseconds.
        After the timeout has expired, the key will automatically be deleted.

        :param milliseconds: the timeout in milliseconds as int or datetime.timedelta.
        :param nx: Set expiry only when the key has no expiry
        :param xx: Set expiry only when the key has an existing expiry
        :param gt: Set expiry only when the new expiry is greater than current one
        :param lt: Set expiry only when the new expiry is less than current one

        Example
        ```python
        # With milliseconds
        redis.set("mykey", "Hello")
        redis.expire("mykey", 500)

        # With a timedelta
        redis.set("mykey", "Hello")
        redis.expire("mykey", datetime.timedelta(milliseconds=500))
        ```

        See https://redis.io/commands/pexpire
        """

        if isinstance(milliseconds, datetime.timedelta):
            # Total seconds returns float, so this is OK.
            milliseconds = int(milliseconds.total_seconds() * 1000)

        command: List = ["PEXPIRE", key, milliseconds]

        if nx:
            command.append("NX")
        if xx:
            command.append("XX")
        if gt:
            command.append("GT")
        if lt:
            command.append("LT")

        return self.execute(command)

    def pexpireat(
        self,
        key: str,
        unix_time_milliseconds: Union[int, datetime.datetime],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> ResponseT:
        """
        Expires a key at a specific unix timestamp (milliseconds).
        After the timeout has expired, the key will automatically be deleted.

        :param unix_time_milliseconds: the timeout in unix miliseconds timestamp as int or a datetime.datetime object.
        :param nx: Set expiry only when the key has no expiry
        :param xx: Set expiry only when the key has an existing expiry
        :param gt: Set expiry only when the new expiry is greater than current one
        :param lt: Set expiry only when the new expiry is less than current one

        Example
        ```python
        # With a unix timestamp
        redis.set("mykey", "Hello")
        redis.pexpireat("mykey", int(time.time() * 1000) )

        # With a datetime object
        redis.set("mykey", "Hello")
        redis.pexpireat("mykey", datetime.datetime.now() + datetime.timedelta(seconds=5))
        ```

        See https://redis.io/commands/pexpireat
        """

        if isinstance(unix_time_milliseconds, datetime.datetime):
            unix_time_milliseconds = int(unix_time_milliseconds.timestamp() * 1000)

        command: List = ["PEXPIREAT", key, unix_time_milliseconds]

        if nx:
            command.append("NX")
        if xx:
            command.append("XX")
        if gt:
            command.append("GT")
        if lt:
            command.append("LT")

        return self.execute(command)

    def pttl(self, key: str) -> ResponseT:
        """
        Returns the milliseconds remaining until the key expires.

        Example:
        ```python
        redis.set("key1", "Hello")

        assert redis.pttl("key1") == -1

        redis.expire("key1", 1000)

        assert redis.pttl("key1") > 0

        redis.persist("key1")

        assert redis.pttl("key1") == -1
        ```

        See https://redis.io/commands/pttl
        """

        command: List = ["PTTL", key]

        return self.execute(command)

    def randomkey(self) -> ResponseT:
        """
        Returns a random key.

        Example:
        ```
        assert redis.randomkey() is None

        redis.set("key1", "Hello")
        redis.set("key2", "World")

        assert redis.randomkey() is not None
        ```

        See https://redis.io/commands/randomkey
        """

        command: List = ["RANDOMKEY"]

        return self.execute(command)

    def rename(self, key: str, newkey: str) -> ResponseT:
        """
        Renames a key and overwrites the new key if it already exists.

        Throws an exception if the key does not exist.

        Example:
        ```
        redis.set("key1", "Hello")
        redis.rename("key1", "key2")

        assert redis.get("key1") is None
        assert redis.get("key2") == "Hello"
        ```

        See https://redis.io/commands/rename
        """

        command: List = ["RENAME", key, newkey]

        return self.execute(command)

    def renamenx(self, key: str, newkey: str) -> ResponseT:
        """
        Renames a key, only if the new key does not exist.

        Throws an exception if the key does not exist.

        Example:
        ```
        redis.set("key1", "Hello")
        redis.set("key2", "World")

        # Rename failed because "key2" already exists.
        assert redis.renamenx("key1", "key2") is False

        assert redis.renamenx("key1", "key3") is True

        assert redis.get("key1") is None
        assert redis.get("key2") == "World"
        assert redis.get("key3") == "Hello"
        ```

        See https://redis.io/commands/renamenx
        """

        command: List = ["RENAMENX", key, newkey]

        return self.execute(command)

    def scan(
        self,
        cursor: int,
        match: Optional[str] = None,
        count: Optional[int] = None,
        type: Optional[str] = None,
    ) -> ResponseT:
        """
        Returns a paginated list of keys matching the pattern.

        :param cursor: the cursor to use for the scan, 0 to start a new scan.
        :param match: a pattern to match.
        :param count: the number of elements to return per page.
        :param type: the type of keys to match. Can be "string", "list", "set", "zset", "hash", or None.

        :return: a tuple of the new cursor and the list of keys.

        Example:
        ```python
        # Get all keys

        cursor = 0
        results = []

        while True:
            cursor, keys = redis.scan(cursor, match="*")

            results.extend(keys)
            if cursor == 0:
                break
        ```

        See https://redis.io/commands/scan
        """

        command: List = ["SCAN", str(cursor)]

        if match is not None:
            command.extend(["MATCH", match])

        if count is not None:
            command.extend(["COUNT", count])

        if type is not None:
            command.extend(["TYPE", type])

        # The raw result is composed of the new cursor and the List of elements.
        return self.execute(command)

    def touch(self, *keys: str) -> ResponseT:
        """
        Alters the last access time of one or more keys

        Example:
        ```python
        redis.set("key1", "Hello")

        assert redis.touch("key1") == 1

        ```

        See https://redis.io/commands/touch
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["TOUCH", *keys]

        return self.execute(command)

    def ttl(self, key: str) -> ResponseT:
        """
        Returns the seconds remaining until the key expires.
        Negative if the key does not exist or does not have an associated timeout.

        ```python
        # Get the TTL of a key
        redis.set("my-key", "value")

        assert redis.ttl("my-key") == -1

        redis.expire("my-key", 10)

        assert redis.ttl("my-key") > 0

        # Non existent key
        assert redis.ttl("non-existent-key") == -2
        ```

        See https://redis.io/commands/ttl
        """

        command: List = ["TTL", key]

        return self.execute(command)

    def type(self, key: str) -> ResponseT:
        """
        Returns the type of the value stored at key.

        Can be "string", "list", "set", "zset", "hash", or "none".

        Example:
        ```
        redis.set("key1", "Hello")

        assert redis.type("key1") == "string"

        redis.lpush("key2", "Hello")

        assert redis.type("key2") == "list"

        assert redis.type("non-existent-key") == "none"
        ```

        See https://redis.io/commands/type
        """

        command: List = ["TYPE", key]

        return self.execute(command)

    def unlink(self, *keys: str) -> ResponseT:
        """
        Deletes one or more keys in a non-blocking way.

        :return: The number of keys that were removed.

        Example:
        ```python
        redis.set("key1", "Hello")

        assert redis.unlink("key1") == 1
        ```

        See https://redis.io/commands/unlink
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["UNLINK", *keys]

        return self.execute(command)

    def geoadd(
        self,
        key: str,
        *members: Tuple[float, float, str],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/geoadd

        :param members: a sequence of (longitude, latitude, name).
        """

        if len(members) == 0:
            raise Exception("At least one member must be added.")

        if nx and xx:
            raise Exception('"nx" and "xx" are mutually exclusive.')

        command: List = ["GEOADD", key]

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        if ch:
            command.append("CH")

        for member in members:
            command.extend(member)

        return self.execute(command)

    def geodist(
        self,
        key: str,
        member1: str,
        member2: str,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
    ) -> ResponseT:
        """
        See https://redis.io/commands/geodist
        """

        command: List = ["GEODIST", key, member1, member2, unit]

        return self.execute(command)

    def geohash(self, key: str, *members: str) -> ResponseT:
        """
        See https://redis.io/commands/geohash
        """

        command: List = ["GEOHASH", key, *members]

        return self.execute(command)

    def geopos(self, key: str, *members: str) -> ResponseT:
        """
        See https://redis.io/commands/geopos
        """

        command: List = ["GEOPOS", key, *members]

        return self.execute(command)

    def georadius(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Optional[int] = None,
        any: bool = False,
        order: Optional[Literal["ASC", "DESC"]] = None,
        store: Optional[str] = None,
        storedist: Optional[str] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/georadius
        """

        handle_georadius_write_exceptions(
            withdist, withhash, withcoord, count, any, store, storedist
        )

        command: List = ["GEORADIUS", key, longitude, latitude, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if order:
            command.append(order)

        if store:
            command.extend(["STORE", store])

        if storedist:
            command.extend(["STOREDIST", storedist])

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def georadius_ro(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Optional[int] = None,
        any: bool = False,
        order: Optional[Literal["ASC", "DESC"]] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/georadius_ro
        """

        if any and count is None:
            raise Exception('"any" can only be used together with "count".')

        command: List = ["GEORADIUS_RO", key, longitude, latitude, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if order:
            command.append(order)

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def georadiusbymember(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Optional[int] = None,
        any: bool = False,
        order: Optional[Literal["ASC", "DESC"]] = None,
        store: Optional[str] = None,
        storedist: Optional[str] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/georadiusbymember
        """

        handle_georadius_write_exceptions(
            withdist, withhash, withcoord, count, any, store, storedist
        )

        command: List = ["GEORADIUSBYMEMBER", key, member, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if order:
            command.append(order)

        if store:
            command.extend(["STORE", store])

        if storedist:
            command.extend(["STOREDIST", storedist])

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def georadiusbymember_ro(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Optional[int] = None,
        any: bool = False,
        order: Optional[Literal["ASC", "DESC"]] = None,
    ) -> ResponseT:
        """
        See https://redis.io/commands/georadiusbymember_ro
        """

        if any and count is None:
            raise Exception('"any" can only be used together with "count".')

        command: List = ["GEORADIUSBYMEMBER_RO", key, member, radius, unit]

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if order:
            command.append(order)

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def geosearch(
        self,
        key: str,
        member: Optional[str] = None,
        longitude: Optional[float] = None,
        latitude: Optional[float] = None,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        radius: Optional[float] = None,
        width: Optional[float] = None,
        height: Optional[float] = None,
        order: Optional[Literal["ASC", "DESC"]] = None,
        count: Optional[int] = None,
        any: bool = False,
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/geosearch
        """

        handle_geosearch_exceptions(
            member,
            longitude,
            latitude,
            radius,
            width,
            height,
            count,
            any,
        )

        command: List = ["GEOSEARCH", key]

        if member is not None:
            command.extend(["FROMMEMBER", member])

        if longitude is not None:
            command.extend(["FROMLONLAT", longitude, latitude])

        if radius is not None:
            command.extend(["BYRADIUS", radius])

        if width is not None:
            command.extend(["BYBOX", width, height])

        command.append(unit)

        if order:
            command.append(order)

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if withdist:
            command.append("WITHDIST")

        if withhash:
            command.append("WITHHASH")

        if withcoord:
            command.append("WITHCOORD")

        # If none of the additional properties are requested, the result will be "List[str]".
        return self.execute(command)

    def geosearchstore(
        self,
        destination: str,
        source: str,
        member: Optional[str] = None,
        longitude: Optional[float] = None,
        latitude: Optional[float] = None,
        unit: Literal["M", "KM", "FT", "MI"] = "M",
        radius: Optional[float] = None,
        width: Optional[float] = None,
        height: Optional[float] = None,
        order: Optional[Literal["ASC", "DESC"]] = None,
        count: Optional[int] = None,
        any: bool = False,
        storedist: bool = False,
    ) -> ResponseT:
        """
        See https://redis.io/commands/geosearchstore
        """

        handle_geosearch_exceptions(
            member,
            longitude,
            latitude,
            radius,
            width,
            height,
            count,
            any,
        )

        command: List = ["GEOSEARCHSTORE", destination, source]

        if member is not None:
            command.extend(["FROMMEMBER", member])

        if longitude is not None:
            command.extend(["FROMLONLAT", longitude, latitude])

        if radius is not None:
            command.extend(["BYRADIUS", radius])

        if width is not None:
            command.extend(["BYBOX", width, height])

        command.append(unit)

        if order:
            command.append(order)

        if count is not None:
            command.extend(["COUNT", count])
            if any:
                command.append("ANY")

        if storedist:
            command.append("STOREDIST")

        return self.execute(command)

    def hdel(self, key: str, *fields: str) -> ResponseT:
        """
        Deletes one or more fields from a hash.

        Returns the number of fields that were removed.

        Example:
        ```python
        redis.hset("myhash", "field1", "Hello")
        redis.hset("myhash", "field2", "World")

        assert redis.hdel("myhash", "field1", "field2") == 2
        ```

        See https://redis.io/commands/hdel
        """

        if len(fields) == 0:
            raise Exception("At least one field must be deleted.")

        command: List = ["HDEL", key, *fields]

        return self.execute(command)

    def hexists(self, key: str, field: str) -> ResponseT:
        """
        Checks if a field exists in a hash.

        Returns True if the field exists, False if it does not.

        See https://redis.io/commands/hexists
        """

        command: List = ["HEXISTS", key, field]

        return self.execute(command)

    def hget(self, key: str, field: str) -> ResponseT:
        """
        Retrieves the value of a field in a hash.

        Returns None if the field or the key does not exist.

        Example:
        ```python
        redis.hset("myhash", "field1", "Hello")

        assert redis.hget("myhash", "field1") == "Hello"
        assert redis.hget("myhash", "field2") is None
        ```

        See https://redis.io/commands/hget
        """

        command: List = ["HGET", key, field]

        return self.execute(command)

    def hgetall(self, key: str) -> ResponseT:
        """
        Returns all fields and values of a hash.

        Example:
        ```python
        redis.hset("myhash", values={
            "field1": "Hello",
            "field2": "World"
        })

        assert redis.hgetall("myhash") == {"field1": "Hello", "field2": "World"}
        ```

        See https://redis.io/commands/hgetall
        """

        command: List = ["HGETALL", key]

        return self.execute(command)

    def hincrby(self, key: str, field: str, increment: int) -> ResponseT:
        """
        Increments the value of a field in a hash by a given amount.

        If the field does not exist, it is set to 0 before performing the operation.

        Returns the new value.

        Example:
        ```python
        redis.hset("myhash", "field1", 5)

        assert redis.hincrby("myhash", "field1", 10) == 15
        ```

        See https://redis.io/commands/hincrby
        """

        command: List = ["HINCRBY", key, field, increment]

        return self.execute(command)

    def hincrbyfloat(self, key: str, field: str, increment: float) -> ResponseT:
        """
        Increments the value of a field in a hash by a given amount.

        If the field does not exist, it is set to 0 before performing the operation.

        Returns the new value.

        Example:
        ```python
        redis.hset("myhash", "field1", 5.5)

        assert redis.hincrbyfloat("myhash", "field1", 10.1) - 15.6 < 0.0001
        ```

        See https://redis.io/commands/hincrbyfloat
        """

        command: List = ["HINCRBYFLOAT", key, field, increment]

        return self.execute(command)

    def hkeys(self, key: str) -> ResponseT:
        """
        Returns all fields in a hash.

        If the hash is empty or does not exist, an empty list is returned.

        Example:
        ```python
        redis.hset("myhash", values={
            "field1": "Hello",
            "field2": "World"
        })

        assert redis.hkeys("myhash") == ["field1", "field2"]
        ```

        See https://redis.io/commands/hkeys
        """

        command: List = ["HKEYS", key]

        return self.execute(command)

    def hlen(self, key: str) -> ResponseT:
        """
        Returns the number of fields in a hash.

        If the hash is empty or does not exist, 0 is returned.

        Example:
        ```python
        assert redis.hlen("myhash") == 0

        redis.hset("myhash", values={
            "field1": "Hello",
            "field2": "World"
        })

        assert redis.hlen("myhash") == 2
        ```

        See https://redis.io/commands/hlen
        """

        command: List = ["HLEN", key]

        return self.execute(command)

    def hmget(self, key: str, *fields: str) -> ResponseT:
        """
        Returns the values of all specified fields in a hash.

        If the hash is empty or does not exist, an empty list is returned.

        Example:
        ```python
        redis.hset("myhash", values={
            "field1": "Hello",
            "field2": "World"
        })

        assert redis.hmget("myhash", "field1", "field2") == ["Hello", "World"]
        ```

        See https://redis.io/commands/hmget
        """

        if len(fields) == 0:
            raise Exception("At least one field must be specified.")

        command: List = ["HMGET", key, *fields]

        return self.execute(command)

    def hmset(self, key: str, values: Mapping[str, ValueT]) -> ResponseT:
        """
        Sets the value of one or multiple fields in a hash.

        Returns the number of fields that were added.

        Example:
        ```python
        # Set multiple fields
        assert redis.hmset("myhash"{
            "field1": "Hello",
            "field2": "World"
        }) == 2
        ```

        See https://redis.io/commands/hmset
        """

        command: List = ["HMSET", key]

        for field, value in values.items():
            command.extend([field, value])

        return self.execute(command)

    def hrandfield(
        self, key: str, count: Optional[int] = None, withvalues: bool = False
    ) -> ResponseT:
        """
        Returns one or more random fields from a hash.

        If the hash is empty or does not exist, an empty list is returned.

        If no count is specified, a single field is returned.
        If a count is specified, a list of fields is returned.
        If "withvalues" is True, a dictionary of fields and values is returned.

        :param key: the key of the hash.
        :param count: the number of fields to return.
        :param withvalues: if True, the keys and values are returned as a dictionary.

        Example:
        ```python
        redis.hset("myhash", values={
            "field1": "Hello",
            "field2": "World"
        })

        # Without count
        assert redis.hrandfield("myhash") in ["field1", "field2"]

        # With count
        assert redis.hrandfield("myhash", count=2) in [
            ["field1", "field2"],
            ["field2", "field1"]
        ]

        # With values
        assert redis.hrandfield("myhash", count=1, withvalues=True) in [
            {"field1": "Hello"},
            {"field2": "World"}
        ]
        ```

        See https://redis.io/commands/hrandfield
        """

        if count is None and withvalues:
            raise Exception('"withvalues" can only be used together with "count"')

        command: List = ["HRANDFIELD", key]

        if count is not None:
            command.extend([count])

            if withvalues:
                command.append("WITHVALUES")

        return self.execute(command)

    def hscan(
        self,
        key: str,
        cursor: int,
        match: Optional[str] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        """
        Returns a paginated list of keys in a hash matching the pattern.

        :param cursor: the cursor to use for the scan, 0 to start a new scan.
        :param match: a pattern to match.
        :param count: the number of elements to return per page.

        :return: a tuple containing the new cursor and the list of keys.

        Example:
        ```python
        # Get all members of a hash.

        cursor = 0
        results = []

        while True:
            cursor, keys = redis.hscan("myhash", cursor, match="*")

            results.extend(keys)
            if cursor == 0:
                break
        ```

        See https://redis.io/commands/hscan
        """

        command: List = ["HSCAN", key, str(cursor)]

        if match is not None:
            command.extend(["MATCH", match])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        return self.execute(command)

    def hset(
        self,
        key: str,
        field: Optional[str] = None,
        value: Optional[ValueT] = None,
        values: Optional[Mapping[str, ValueT]] = None,
    ) -> ResponseT:
        """
        Sets the value of one or multiple fields in a hash.

        Returns the number of fields that were added.

        `hmset` can be used to set multiple fields at once too.

        Example:
        ```python

        # Set a single field
        assert redis.hset("myhash", "field1", "Hello") == 1

        # Set multiple fields
        assert redis.hset("myhash", values={
            "field1": "Hello",
            "field2": "World"
        }) == 2
        ```

        See https://redis.io/commands/hset
        """
        command: List = ["HSET", key]

        if field is None and values is None:
            raise Exception("'hset' with no key value pairs")

        if field and value:
            command.extend([field, value])

        if values is not None:
            for field, value in values.items():
                command.extend([field, value])

        return self.execute(command)

    def hsetnx(self, key: str, field: str, value: ValueT) -> ResponseT:
        """
        Sets the value of a field in a hash, only if the field does not exist.

        Returns True if the field was set, False if it was not set.

        Example:
        ```python
        assert redis.hsetnx("myhash", "field1", "Hello") is True
        assert redis.hsetnx("myhash", "field1", "World") is False
        ```

        See https://redis.io/commands/hsetnx
        """

        command: List = ["HSETNX", key, field, value]

        return self.execute(command)

    def hstrlen(self, key: str, field: str) -> ResponseT:
        """
        Returns the length of a value in a hash.

        Returns 0 if the field does not exist or the key does not exist.

        Example:
        ```python
        redis.hset("myhash", "field1", "Hello")

        assert redis.hstrlen("myhash", "field1") == 5
        assert redis.hstrlen("myhash", "field2") == 0
        ```

        See https://redis.io/commands/hstrlen
        """

        command: List = ["HSTRLEN", key, field]

        return self.execute(command)

    def hvals(self, key: str) -> ResponseT:
        """
        Returns all values in a hash.

        If the hash is empty or does not exist, an empty list is returned.

        Example:
        ```python
        redis.hset("myhash", values={
            "field1": "Hello",
            "field2": "World"
        })

        assert redis.hvals("myhash") == ["Hello", "World"]
        ```

        See https://redis.io/commands/hvals
        """

        command: List = ["HVALS", key]

        return self.execute(command)

    def pfadd(self, key: str, *elements: ValueT) -> ResponseT:
        """
        See https://redis.io/commands/pfadd
        """

        command: List = ["PFADD", key, *elements]

        return self.execute(command)

    def pfcount(self, *keys: str) -> ResponseT:
        """
        See https://redis.io/commands/pfcount
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["PFCOUNT", *keys]

        return self.execute(command)

    def pfmerge(self, destkey: str, *sourcekeys: str) -> ResponseT:
        """
        See https://redis.io/commands/pfmerge
        """

        command: List = ["PFMERGE", destkey, *sourcekeys]

        return self.execute(command)

    def lindex(self, key: str, index: int) -> ResponseT:
        """
        Returns the element at index in the list stored at key.o

        See https://redis.io/commands/lindex
        """

        command: List = ["LINDEX", key, index]

        return self.execute(command)

    def linsert(
        self,
        key: str,
        where: Literal["BEFORE", "AFTER"],
        pivot: ValueT,
        element: str,
    ) -> ResponseT:
        """
        Inserts an element before or after another element in a list.

        :param key: the key of the list.
        :param where: whether to insert before or after the pivot.
        :param pivot: the element to insert before or after.
        :param element: the element to insert.

        See https://redis.io/commands/linsert
        """

        command: List = ["LINSERT", key, where, pivot, element]

        return self.execute(command)

    def llen(self, key: str) -> ResponseT:
        """
        Returns the length of a list.

        See https://redis.io/commands/llen
        """

        command: List = ["LLEN", key]

        return self.execute(command)

    def lmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["LEFT", "RIGHT"] = "LEFT",
        whereto: Literal["LEFT", "RIGHT"] = "RIGHT",
    ) -> ResponseT:
        """
        Moves an element from one list to another atomically.

        :param source: the source list.
        :param destination: the destination list.
        :param wherefrom: The side to pop from. Can be "LEFT" or "RIGHT".
        :param whereto: The side to push to. Can be "LEFT" or "RIGHT".

        Example:
        ```python
        redis.rpush("source", "one", "two", "three")
        redis.lpush("destination", "four", "five", "six")

        assert redis.lmove("source", "destination", "RIGHT", "LEFT") == "three"

        assert redis.lrange("source", 0, -1) == ["one", "two"]
        ```

        See https://redis.io/commands/lmove
        """

        command: List = [
            "LMOVE",
            source,
            destination,
            wherefrom,
            whereto,
        ]

        return self.execute(command)

    def lpop(self, key: str, count: Optional[int] = None) -> ResponseT:
        """
        Removes and returns the first element of a list.

        If count is specified, multiple elements are popped from the left side of the list.

        Returns a single element if count is not specified.
        Returns a list of elements if count is specified.
        If the list is empty or does not exist, None is returned.

        Example:
        ```python
        # Single
        redis.rpush("mylist", "one", "two", "three")

        assert redis.lpop("mylist") == "one"

        # Multiple
        redis.rpush("mylist", "one", "two", "three")

        assert redis.lpop("mylist", 2) == ["one", "two"]
        ```

        See https://redis.io/commands/lpop
        """

        command: List = ["LPOP", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def lpos(
        self,
        key: str,
        element: ValueT,
        rank: Optional[int] = None,
        count: Optional[int] = None,
        maxlen: Optional[int] = None,
    ) -> ResponseT:
        """
        Returns the index of matching elements inside a list.

        :param key: the key of the list.
        :param element: the element to search for.
        :param rank: which match to return. 1 to return the first match, 0 to return the second match, and so on.
        :param count: the maximum number of matches to return.
        :param maxlen: the maximum number of elements to scan.

        Returns the index of the matching element or array of indexes if count is specified.
        If the element does not exist, None is returned.

        Example:
        ```py
        redis.rpush("key", "a", "b", "c");

        assert redis.lpos("key", "b") == 1

        # With Rank
        redis.rpush("key", "a", "b", "c", "b");

        assert redis.lpos("key", "b", rank=2) == 3

        # With Count
        redis.rpush("key", "a", "b", "b")

        assert redis.lpos("key", "b", count=2) == [1, 2]
        ```

        See https://redis.io/commands/lpos
        """

        command: List = ["LPOS", key, element]

        if rank is not None:
            command.extend(["RANK", rank])

        if count is not None:
            command.extend(["COUNT", count])

        if maxlen is not None:
            command.extend(["MAXLEN", maxlen])

        return self.execute(command)

    def lpush(self, key: str, *elements: ValueT) -> ResponseT:
        """
        Pushes an element to the left side of a list.

        Returns the new length of the list.

        Example:
        ```python
        assert redis.lpush("mylist", "one", "two", "three") == 3

        assert lrange("mylist", 0, -1) == ["three", "two", "one"]
        ```

        See https://redis.io/commands/lpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["LPUSH", key, *elements]

        return self.execute(command)

    def lpushx(self, key: str, *elements: ValueT) -> ResponseT:
        """
        Pushes an element to the left side of a list, only if the list exists.

        Returns the new length of the list.
        If the list does not exist, 0 is returned.

        Example:
        ```python
        # Initialize the list
        redis.lpush("mylist", "one")

        assert redis.lpushx("mylist", "two", "three") == 3

        assert lrange("mylist", 0, -1) == ["three", "two", "one"]
        ```

        See https://redis.io/commands/lpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["LPUSHX", key, *elements]

        return self.execute(command)

    def lrange(self, key: str, start: int, stop: int) -> ResponseT:
        """
        Returns a range of elements from a list.

        If start or stop are negative, the index is calculated from the end of the list.

        :param key: the key of the list.
        :param start: the index of the first element to return.
        :param stop: the index of the last element to return.

        Returns a list of elements.

        Example:
        ```python
        redis.rpush("mylist", "one", "two", "three")

        assert redis.lrange("mylist", 0, 1) == ["one", "two"]

        assert redis.lrange("mylist", 0, -1) == ["one", "two", "three"]
        ```

        See https://redis.io/commands/lrange
        """

        command: List = ["LRANGE", key, start, stop]

        return self.execute(command)

    def lrem(self, key: str, count: int, element: ValueT) -> ResponseT:
        """
        Removes the first `count` occurrences of an element from a list.

        :param key: the key of the list.
        :param count: the number of occurrences to remove.
        :param element: the element to remove.

        Returns the number of elements that were removed.

        Example:
        ```python
        redis.rpush("mylist", "one", "two", "three", "two", "one")

        assert redis.lrem("mylist", 2, "two") == 2

        assert redis.lrange("mylist", 0, -1) == ["one", "three", "one"]
        ```

        See https://redis.io/commands/lrem
        """

        command: List = ["LREM", key, count, element]

        return self.execute(command)

    def lset(self, key: str, index: int, element: ValueT) -> ResponseT:
        """
        Sets the value of an element in a list by its index.

        Returns True if the element was set, False if the index is out of range.

        Example:
        ```python
        redis.rpush("mylist", "one", "two", "three")

        assert redis.lset("mylist", 1, "Hello") is True

        assert redis.lrange("mylist", 0, -1) == ["one", "Hello", "three"]

        assert redis.lset("mylist", 5, "Hello") is False

        assert redis.lrange("mylist", 0, -1) == ["one", "Hello", "three"]
        ```

        See https://redis.io/commands/lset
        """

        command: List = ["LSET", key, index, element]

        return self.execute(command)

    def ltrim(self, key: str, start: int, stop: int) -> ResponseT:
        """
        Trims a list to the specified range.

        :param key: the key of the list.
        :param start: the index of the first element to keep.
        :param stop: the index of the last element to keep.

        Returns True if the list was trimmed, False if the key does not exist.

        Example:
        ```python
        redis.rpush("mylist", "one", "two", "three")

        assert redis.ltrim("mylist", 0, 1) is True

        assert redis.lrange("mylist", 0, -1) == ["one", "two"]
        ```

        See https://redis.io/commands/ltrim
        """

        command: List = ["LTRIM", key, start, stop]

        return self.execute(command)

    def rpop(self, key: str, count: Optional[int] = None) -> ResponseT:
        """
        Removes and returns the last element of a list.

        If count is specified, multiple elements are popped from the right side of the list.

        Returns a single element if count is not specified.
        Returns a list of elements if count is specified.

        Example:
        ```python
        # Single
        redis.rpush("mylist", "one", "two", "three")

        assert redis.rpop("mylist") == "three"

        # Multiple
        redis.rpush("mylist", "one", "two", "three")

        assert redis.rpop("mylist", 2) == ["three", "two"]
        ```

        See https://redis.io/commands/rpop
        """

        command: List = ["RPOP", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def rpoplpush(self, source: str, destination: str) -> ResponseT:
        """
        Deletes an element from the right side of a list and pushes it to the left side of another list.

        Deprecated since Redis 6. Use `lmove` with `wherefrom="RIGHT"` and `whereto="LEFT"` instead.

        Example:
        ```py
        redis.rpush("source", "one", "two", "three")

        assert redis.rpoplpush("source", "destination") == "three"

        assert redis.lrange("source", 0, -1) == ["one", "two"]
        ```

        See https://redis.io/commands/rpoplpush
        """

        command: List = ["RPOPLPUSH", source, destination]

        return self.execute(command)

    def rpush(self, key: str, *elements: ValueT) -> ResponseT:
        """
        Pushes an element to the right side of a list.

        Returns the new length of the list.

        Example:
        ```python
        assert redis.rpush("mylist", "one", "two", "three") == 3

        assert redis.lrange("mylist", 0, -1) == ["one", "two", "three"]
        ```

        See https://redis.io/commands/rpush
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["RPUSH", key, *elements]

        return self.execute(command)

    def rpushx(self, key: str, *elements: ValueT) -> ResponseT:
        """
        Pushes an element to the right side of a list only if the list exists.

        Returns the new length of the list.
        Returns 0 if the list does not exist.

        Example:
        ```python
        assert redis.rpushx("mylist", "one", "two", "three") == 3

        assert redis.lrange("mylist", 0, -1) == ["one", "two", "three"]
        ```

        See https://redis.io/commands/rpushx
        """

        if len(elements) == 0:
            raise Exception("At least one element must be added.")

        command: List = ["RPUSHX", key, *elements]

        return self.execute(command)

    def publish(self, channel: str, message: ValueT) -> ResponseT:
        """
        Publishes a message to a channel.

        Returns the number of clients that received the message.

        Example:
        ```python
        redis.publish("mychannel", "Hello")
        ```

        See https://redis.io/commands/publish
        """

        command: List = ["PUBLISH", channel, message]

        return self.execute(command)

    def eval(
        self,
        script: str,
        keys: Optional[List[str]] = None,
        args: Optional[List[str]] = None,
    ) -> ResponseT:
        """
        Evaluates a Lua script in the server

        :param script: the Lua script.
        :param keys: the list of keys that can be referenced in the script.
        :param args: the list of arguments that can be referenced in the script.

        Returns the result of the script.

        Example:
        ```python
        assert redis.eval("return 1 + 1") == 2

        assert redis.eval("return KEYS[1]", keys=["mykey"]) == "mykey"

        assert redis.eval("return ARGV[1]", args=["Hello"]) == "Hello"

        script = \"\"\"
        local value = redis.call("GET", KEYS[1])
        return value
        \"\"\"

        redis.set("mykey", "Hello")

        assert redis.eval(script, keys=["mykey"]) == "Hello"
        ```

        See https://redis.io/commands/eval

        The number of keys is calculated automatically.
        """

        command: List = ["EVAL", script]

        if keys is not None:
            command.extend([len(keys), *keys])
        else:
            command.append(0)

        if args:
            command.extend(args)

        return self.execute(command)

    def eval_ro(
        self,
        script: str,
        keys: Optional[List[str]] = None,
        args: Optional[List[str]] = None,
    ) -> ResponseT:
        """
        Evaluates a read-only Lua script in the server

        :param script: the read-only Lua script.
        :param keys: the list of keys that can be referenced in the script.
        :param args: the list of arguments that can be referenced in the script.

        Returns the result of the script.

        Example:
        ```python
        assert redis.eval_ro("return 1 + 1") == 2

        assert redis.eval_ro("return KEYS[1]", keys=["mykey"]) == "mykey"

        assert redis.eval_ro("return ARGV[1]", args=["Hello"]) == "Hello"

        script = \"\"\"
        local value = redis.call("GET", KEYS[1])
        return value
        \"\"\"

        redis.set("mykey", "Hello")

        assert redis.eval_ro(script, keys=["mykey"]) == "Hello"
        ```

        See https://redis.io/commands/eval_ro

        The number of keys is calculated automatically.
        """

        command: List = ["EVAL_RO", script]

        if keys is not None:
            command.extend([len(keys), *keys])
        else:
            command.append(0)

        if args:
            command.extend(args)

        return self.execute(command)

    def evalsha(
        self,
        sha1: str,
        keys: Optional[List[str]] = None,
        args: Optional[List[str]] = None,
    ) -> ResponseT:
        """
        Evaluates a Lua script in the server, cached by its SHA1 digest.

        :param sha1: the SHA1 digest of the script.
        :param keys: the list of keys that can be referenced in the script.
        :param args: the list of arguments that can be referenced in the script.

        Returns the result of the script.

        Example:
        ```python
        result = redis.eval("fb67a0c03b48ddbf8b4c9b011e779563bdbc28cb", args=["hello"])
        assert result = "hello"
        ```

        See https://redis.io/commands/evalsha

        The number of keys is calculated automatically.
        """

        command: List = ["EVALSHA", sha1]

        if keys is not None:
            command.extend([len(keys), *keys])
        else:
            command.append(0)

        if args:
            command.extend(args)

        return self.execute(command)

    def evalsha_ro(
        self,
        sha1: str,
        keys: Optional[List[str]] = None,
        args: Optional[List[str]] = None,
    ) -> ResponseT:
        """
        Evaluates a read-only Lua script in the server, cached by its SHA1 digest.

        :param sha1: the SHA1 digest of the read-only script.
        :param keys: the list of keys that can be referenced in the script.
        :param args: the list of arguments that can be referenced in the script.

        Returns the result of the script.

        Example:
        ```python
        result = redis.evalsha_ro("fb67a0c03b48ddbf8b4c9b011e779563bdbc28cb", args=["hello"])
        assert result = "hello"
        ```

        See https://redis.io/commands/evalsha_ro

        The number of keys is calculated automatically.
        """

        command: List = ["EVALSHA_RO", sha1]

        if keys is not None:
            command.extend([len(keys), *keys])
        else:
            command.append(0)

        if args:
            command.extend(args)

        return self.execute(command)

    def dbsize(self) -> ResponseT:
        """
        Returns the number of keys in the database.

        See https://redis.io/commands/dbsize
        """

        command: List = ["DBSIZE"]

        return self.execute(command)

    def flushall(
        self, flush_type: Optional[Literal["ASYNC", "SYNC"]] = None
    ) -> ResponseT:
        """
        See https://redis.io/commands/flushall
        """

        command: List = ["FLUSHALL"]

        if flush_type:
            command.append(flush_type)

        return self.execute(command)

    def flushdb(
        self, flush_type: Optional[Literal["ASYNC", "SYNC"]] = None
    ) -> ResponseT:
        """
        See https://redis.io/commands/flushdb
        """

        command: List = ["FLUSHDB"]

        if flush_type:
            command.append(flush_type)

        return self.execute(command)

    def time(self) -> ResponseT:
        """
        See https://redis.io/commands/time
        """

        command: List = ["TIME"]

        return self.execute(command)

    def sadd(self, key: str, *members: ValueT) -> ResponseT:
        """
        Adds one or more members to a set.

        Returns the number of members that were added.

        Example:
        ```python
        assert redis.sadd("myset", "one", "two", "three") == 3

        # Only newly added members are counted
        assert redis.sadd("myset", "one", "two", "four") == 1
        ```

        See https://redis.io/commands/sadd
        """

        if len(members) == 0:
            raise Exception("At least one member must be added.")

        command: List = ["SADD", key, *members]

        return self.execute(command)

    def scard(self, key: str) -> ResponseT:
        """
        Returns how many members are in a set

        If the set does not exist, 0 is returned.

        See https://redis.io/commands/scard
        """

        command: List = ["SCARD", key]

        return self.execute(command)

    def sdiff(self, *keys: str) -> ResponseT:
        """
        Returns the difference between multiple sets.

        If a key does not exist, it is treated as an empty set.

        Example:
        ```python
        redis.sadd("key1", "a", "b", "c")

        redis.sadd("key2", "c", "d", "e")

        assert redis.sdiff("key1", "key2") == {"a", "b"}
        ```

        See https://redis.io/commands/sdiff
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SDIFF", *keys]

        return self.execute(command)

    def sdiffstore(self, destination: str, *keys: str) -> ResponseT:
        """
        Returns the difference between multiple sets and stores it in a new set.

        If a key does not exist, it is treated as an empty set.

        Returns the number of elements in the resulting set.

        Example:
        ```python
        redis.sadd("key1", "a", "b", "c")

        redis.sadd("key2", "c", "d", "e")

        # Store the result in a new set
        assert redis.sdiffstore("res", "key1", "key2") == 2

        assert redis.smembers("set") == {"a", "b"}
        ```

        See https://redis.io/commands/sdiffstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SDIFFSTORE", destination, *keys]

        return self.execute(command)

    def sinter(self, *keys: str) -> ResponseT:
        """
        Returns the intersection between multiple sets.

        If a key does not exist, it is treated as an empty set.

        Example:
        ```python
        redis.sadd("set1", "a", "b", "c");
        redis.sadd("set2", "c", "d", "e");

        assert redis.sinter("set1", "set2") == {"c"}
        ```

        See https://redis.io/commands/sinter
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SINTER", *keys]

        return self.execute(command)

    def sinterstore(self, destination: str, *keys: str) -> ResponseT:
        """
        Calculates the intersection between multiple sets and stores it in a new set.

        If a key does not exist, it is treated as an empty set.

        Returns the number of elements in the resulting set.

        Example:
        ```
        redis.sadd("set1", "a", "b", "c");

        redis.sadd("set2", "c", "d", "e");

        assert redis.sinter("destination", "set1", "set2") == 1
        ```

        See https://redis.io/commands/sinterstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SINTERSTORE", destination, *keys]

        return self.execute(command)

    def sismember(self, key: str, member: ValueT) -> ResponseT:
        """
        Checks if a member is in a set.

        Returns True if the member is in the set, False if it is not.

        If the set does not exist, False is returned.

        See https://redis.io/commands/sismember
        """

        command: List = ["SISMEMBER", key, member]

        return self.execute(command)

    def smembers(self, key: str) -> ResponseT:
        """
        Returns all members of a set.

        See https://redis.io/commands/smembers
        """

        command: List = ["SMEMBERS", key]

        return self.execute(command)

    def smismember(self, key: str, *members: ValueT) -> ResponseT:
        """
        Checks if multiple members are in a set.

        Returns a list of booleans indicating whether the members are in the set.

        If the set does not exist, an empty list is returned.

        Example:
        ```python
        redis.sadd("myset", "one", "two", "three")

        assert redis.smismember("myset", "one", "four") == [True, False]

        assert redis.smismember("myset", "four", "five") == [False, False]
        ```

        See https://redis.io/commands/smismember
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["SMISMEMBER", key, *members]

        return self.execute(command)

    def smove(self, source: str, destination: str, member: ValueT) -> ResponseT:
        """
        Moves a member from one set to another atomically.

        Returns True if the member was moved, False if it was not.

        Example:
        ```python
        redis.sadd("src", "one", "two", "three")

        redis.sadd("dest", "four")

        assert redis.smove("src", "dest", "three") is True

        assert redis.smembers("source") == {"one", "two"}

        assert redis.smembers("destination") == {"three", "four"}
        ```

        See https://redis.io/commands/smove
        """

        command: List = ["SMOVE", source, destination, member]

        return self.execute(command)

    def spop(self, key: str, count: Optional[int] = None) -> ResponseT:
        """
        Removes and returns one or more random members from a set.

        If count is specified, multiple members are popped from the set.

        Returns a single member if count is not specified.

        Returns a list of members if count is specified.

        If the set is empty or does not exist, None is returned.

        Example:
        ```python
        # Single
        redis.sadd("myset", "one", "two", "three")

        assert redis.spop("myset") in {"one", "two", "three"}

        # Multiple
        redis.sadd("myset", "one", "two", "three")

        assert redis.spop("myset", 2) in {"one", "two", "three"}
        ```

        See https://redis.io/commands/spop
        """

        command: List = ["SPOP", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def srandmember(self, key: str, count: Optional[int] = None) -> ResponseT:
        """
        Returns one or more random members from a set.

        If count is specified, multiple members are returned from the set.

        Returns a single member if count is not specified.

        Returns a list of members if count is specified.

        If the set is empty or does not exist, None is returned.

        Example:
        ```python
        # Single
        redis.sadd("myset", "one", "two", "three")

        assert redis.srandmember("myset") in {"one", "two", "three"}

        # Multiple
        redis.sadd("myset", "one", "two", "three")

        assert redis.srandmember("myset", 2) in {"one", "two", "three"}
        ```

        See https://redis.io/commands/srandmember
        """

        command: List = ["SRANDMEMBER", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def srem(self, key: str, *members: ValueT) -> ResponseT:
        """
        Removes one or more members from a set.

        Returns the number of members that were removed.

        Example:
        ```python
        redis.sadd("myset", "one", "two", "three")

        assert redis.srem("myset", "one", "four") == 1
        ```

        See https://redis.io/commands/srem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["SREM", key, *members]

        return self.execute(command)

    def sscan(
        self,
        key: str,
        cursor: int = 0,
        match: Optional[str] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        """
        Returns a paginated list of members of a set matching a pattern.

        :param cursor: the cursor to use for the scan, 0 to start a new scan.
        :param match: a pattern to match.
        :param count: the number of elements to return per page.

        :return: a tuple containing the new cursor and the list of elements.

        Example:
        ```python
        # Get all members of a set.

        cursor = 0
        results = []

        while True:
            cursor, keys = redis.sscan("myset", cursor, match="*")

            results.extend(keys)
            if cursor == 0:
                break
        ```

        See https://redis.io/commands/sscan
        """

        command: List = ["SSCAN", key, str(cursor)]

        if match is not None:
            command.extend(["MATCH", match])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        return self.execute(command)

    def sunion(self, *keys: str) -> ResponseT:
        """
        Returns the union between multiple sets.

        If a key does not exist, it is treated as an empty set.

        Example:
        ```python
        redis.sadd("key1", "a", "b", "c")

        redis.sadd("key2", "c", "d", "e")

        assert redis.sunion("key1", "key2") == {"a", "b", "c", "d", "e"}
        ```

        See https://redis.io/commands/sunion
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SUNION", *keys]

        return self.execute(command)

    def sunionstore(self, destination: str, *keys: str) -> ResponseT:
        """
        Calculates the union between multiple sets and stores it in a new set.

        Returns the number of elements in the resulting set.

        Example:
        ```python
        redis.sadd("key1", "a", "b", "c")

        redis.sadd("key2", "c", "d", "e")

        assert redis.sunionstore("destination", "key1", "key2") == 5
        ```

        See https://redis.io/commands/sunionstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["SUNIONSTORE", destination, *keys]

        return self.execute(command)

    def zadd(
        self,
        key: str,
        scores: Dict[str, float],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
        incr: bool = False,
    ) -> ResponseT:
        """
        Adds one or more members to a sorted set, or updates its score if it already exists.

        Returns the number of elements that were added.

        :param key: the key of the sorted set.
        :param scores: a dictionary of members and their scores.
        :param nx: only add new members, do not update scores for members that already exist.
        :param xx: only update scores for members that already exist, do not add new members.
        :param gt: only update scores if the new score is greater than the current score.
        :param lt: only update scores if the new score is less than the current score.
        :param ch: return the number of elements that were changed instead of the number of elements that were added.
        :param incr: when this option is specified, the score is used as an increment instead of being set to the specified value. Only one score can be specified in this mode.

        Example:
        ```python
        # Add three elements
        assert redis.zadd("myset", {"one": 1, "two": 2, "three": 3}) == 3

        # No element is added since "one" and "two" already exist
        assert redis.zadd("myset", {"one": 1, "two": 2}, nx=True) == 0

        # New element is not added since it does not exist
        assert redis.zadd("myset", {"new-element": 1}, xx=True) == 0

        # Only "three" is updated since new score was greater
        assert redis.zadd("myset", {"three": 10, "two": 0}, gt=True) == 1

        # Only "three" is updated since new score was greater
        assert redis.zadd("myset", {"three": 10, "two": 0}, gt=True) == 1
        ```

        See https://redis.io/commands/zadd
        """

        if nx and xx:
            raise Exception('"nx" and "xx" are mutually exclusive.')

        if gt and lt:
            raise Exception('"gt" and "lt" are mutually exclusive.')

        if nx and (gt or lt):
            raise Exception('"nx" and "gt" or "lt" are mutually exclusive.')

        command: List = ["ZADD", key]

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        if gt:
            command.append("GT")

        if lt:
            command.append("LT")

        if ch:
            command.append("CH")

        if incr:
            command.append("INCR")

        for name, score in scores.items():
            command.extend([score, name])

        return self.execute(command)

    def zcard(self, key: str) -> ResponseT:
        """
        Returns the number of elements in a sorted set.

        Example:
        ```python
        redis.zadd("myset", {"one": 1, "two": 2, "three": 3})

        assert redis.zcard("myset") == 3
        ```

        See https://redis.io/commands/zcard
        """

        command: List = ["ZCARD", key]

        return self.execute(command)

    def zcount(self, key: str, min: FloatMinMaxT, max: FloatMinMaxT) -> ResponseT:
        """
        Counts the number of elements in a sorted set with scores within the given values.

        If you need to use "-inf" and "+inf", please write them as strings.

        :param key: the key of the sorted set.
        :param min: the minimum score to include.
        :param max: the maximum score to include.

        Example:
        ```python
        redis.zadd("myset", {"one": 1, "two": 2, "three": 3})

        assert redis.zcount("myset", 1, 2) == 2

        assert redis.zcount("myset", "-inf", "+inf") == 3
        ```

        See https://redis.io/commands/zcount
        """

        command: List = ["ZCOUNT", key, min, max]

        return self.execute(command)

    def zdiff(self, keys: List[str], withscores: bool = False) -> ResponseT:
        """
        Returns the difference between multiple sorted sets.

        If a key does not exist, it is treated as an empty sorted set.

        Example:
        ```python
        redis.zadd("key1", {"a": 1, "b": 2, "c": 3})

        redis.zadd("key2", {"c": 3, "d": 4, "e": 5})

        result = redis.zdiff(["key1", "key2"])

        assert result == ["a", "b"]

        result = redis.zdiff(["key1", "key2"], withscores=True)

        assert result == [("a", 1), ("b", 2)]
        ```

        See https://redis.io/commands/zdiff
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZDIFF", len(keys), *keys]

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zdiffstore(self, destination: str, keys: List[str]) -> ResponseT:
        """
        Calculates the difference between multiple sorted sets and stores it in a new sorted set.


        Example:
        ```py
        redis.zadd("key1", {"a": 1, "b": 2, "c": 3})

        redis.zadd("key2", {"c": 3, "d": 4, "e": 5})

        # a and b
        assert redis.zdiffstore("dest", ["key1", "key2"]) == 2
        ```

        See https://redis.io/commands/zdiffstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZDIFFSTORE", destination, len(keys), *keys]

        return self.execute(command)

    def zincrby(self, key: str, increment: float, member: str) -> ResponseT:
        """
        Increments the score of a member in a sorted set.

        Returns the new score of the member.

        Example:
        ```python
        redis.zadd("myset", {"one": 1, "two": 2, "three": 3})

        assert redis.zincrby("myset", 2, "one") == 3
        ```

        See https://redis.io/commands/zincrby
        """

        command: List = ["ZINCRBY", key, increment, member]

        return self.execute(command)

    def zinter(
        self,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Optional[Literal["SUM", "MIN", "MAX"]] = None,
        withscores: bool = False,
    ) -> ResponseT:
        """
        Returns the intersection between multiple sorted sets.

        If aggregate is specified, the resulting scores are aggregated using the specified method.

        :param keys: the keys of the sorted sets.
        :param weights: the weights to apply to the sorted sets.
        :param aggregate: the method to use to aggregate scores.
        :param withscores: whether to return the scores along with the members.

        Returns a list of members or a list of (member, score) tuples if withscores is True.

        If an aggregate method is specified, the scores are aggregated.

        Example:
        ```python
        redis.zadd("key1", {"a": 1, "b": 2, "c": 3})

        redis.zadd("key2", {"c": 3, "d": 4, "e": 5})

        result = redis.zinter(["key1", "key2"])

        assert result == ["c"]
        ```

        See https://redis.io/commands/zinter
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZINTER", len(keys), *keys]

        if weights:
            command.extend(["WEIGHTS", *weights])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zinterstore(
        self,
        destination: str,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Optional[Literal["SUM", "MIN", "MAX"]] = None,
    ) -> ResponseT:
        """
        Calculates the intersection between multiple sorted sets and stores it in a key.

        If aggregate is specified, the resulting scores are aggregated using the specified method.

        :param keys: the keys of the sorted sets.
        :param weights: the weights to apply to the sorted sets.
        :param aggregate: the method to use to aggregate scores.
        :param withscores: whether to return the scores along with the members.

        Returns the number of elements in the resulting sorted set.

        Example:
        ```python
        redis.zadd("key1", {"a": 1, "b": 2, "c": 3})

        redis.zadd("key2", {"c": 3, "d": 4, "e": 5})

        result = redis.zinterstore("dest", ["key1", "key2"])

        assert result == 1
        ```

        See https://redis.io/commands/zinterstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZINTERSTORE", destination, len(keys), *keys]

        if weights:
            command.extend(["WEIGHTS", *weights])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        return self.execute(command)

    def zlexcount(self, key: str, min: str, max: str) -> ResponseT:
        """
        Counts the number of elements in a sorted set between a min and max value.

        :param key: the key of the sorted set.
        :param min: the minimum value to include.
        :param max: the maximum value to include.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zlexcount("myset", "-", "+") == 3
        ```

        See https://redis.io/commands/zlexcount
        """

        if not min.startswith(("(", "[", "+", "-")) or not max.startswith(
            ("(", "[", "+", "-")
        ):
            raise Exception(
                "\"min\" and \"max\" must either start with '(' or '[' or be '+' or '-'."
            )

        command: List = ["ZLEXCOUNT", key, min, max]

        return self.execute(command)

    def zmscore(self, key: str, members: List[str]) -> ResponseT:
        """
        Returns the scores associated with the specified members in a sorted set.

        If a member does not exist, None is returned for it.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zmscore("myset", ["a", "b", "c", "d"]) == [1, 2, 3, None]
        ```

        See https://redis.io/commands/zmscore
        """

        if len(members) == 0:
            raise Exception("At least one member must be specified.")

        command: List = ["ZMSCORE", key, *members]

        return self.execute(command)

    def zpopmax(self, key: str, count: Optional[int] = None) -> ResponseT:
        """
        Removes and returns the members with the highest scores in a sorted set.

        Returns a list member score tuples.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zpopmax("myset") == [("c", 3)]
        ```

        See https://redis.io/commands/zpopmax
        """

        command: List = ["ZPOPMAX", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def zpopmin(self, key: str, count: Optional[int] = None) -> ResponseT:
        """
        Removes and returns the members with the lowest scores in a sorted set.

        Returns a list member score tuples.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zpopmin("myset") == [("a", 1)]
        ```

        See https://redis.io/commands/zpopmin
        """

        command: List = ["ZPOPMIN", key]

        if count is not None:
            command.append(count)

        return self.execute(command)

    def zrandmember(
        self, key: str, count: Optional[int] = None, withscores: bool = False
    ) -> ResponseT:
        """
        Returns one or more random members from a sorted set.

        If count is specified, multiple members are returned from the set.

        Returns a single member if count is not specified.

        If withscores is True, the scores are returned along with the members as a tuple.

        Example:
        ```python
        redis.zadd("myset", {"one": 1, "two": 2, "three": 3})

        # "one"
        redis.zrandmember("myset")

        # ["one", "three"]
        redis.zrandmember("myset", 2)
        ```

        See https://redis.io/commands/zrandmember
        """

        if count is None and withscores:
            raise Exception('"withscores" can only be used with "count".')

        command: List = ["ZRANDMEMBER", key]

        if count is not None:
            command.append(count)

            if withscores:
                command.append("WITHSCORES")

        return self.execute(command)

    def zrange(
        self,
        key: str,
        start: FloatMinMaxT,
        stop: FloatMinMaxT,
        sortby: Optional[Literal["BYSCORE", "BYLEX"]] = None,
        rev: bool = False,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        withscores: bool = False,
    ) -> ResponseT:
        """
        Returns the members of a sorted set between a min and max value.

        :param key: the key of the sorted set.
        :param start: the minimum value to include.
        :param stop: the maximum value to include.
        :param sortby: whether to sort by score or lexicographically.
        :param rev: whether to reverse the results.
        :param offset: the offset to start from.
        :param count: the number of elements to return.
        :param withscores: whether to return the scores along with the members.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zrange("myset", 0, 1) == ["a", "b"]

        assert redis.zrange("myset", 0, 1, rev=True) == ["c", "b"]

        assert redis.zrange("myset", 0, 1, sortby="BYSCORE") == ["a", "b"]

        # With scores
        assert redis.zrange("myset", 0, 1, withscores=True) == [("a", 1), ("b", 2)]
        ```

        See https://redis.io/commands/zrange
        """

        handle_non_deprecated_zrange_exceptions(sortby, start, stop, offset, count)

        command: List = ["ZRANGE", key, start, stop]

        if sortby:
            command.append(sortby)

        if rev:
            command.append("REV")

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zrangebylex(
        self,
        key: str,
        min: str,
        max: str,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        """
        Returns the members of a sorted set between a min and max value ordered lexicographically.

        Deprecated: use zrange with sortby="BYLEX" instead.

        :param key: the key of the sorted set.
        :param min: the minimum value to include. Can be "[a", "(a", "+", or "-".
        :param max: the maximum value to include. Can be "[a", "(a", "+", or "-".
        :param offset: the offset to start from.
        :param count: the number of elements to return.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zrangebylex("myset", "-", "+") == ["a", "b", "c"]
        ```

        See https://redis.io/commands/zrangebylex
        """

        handle_zrangebylex_exceptions(min, max, offset, count)

        command: List = ["ZRANGEBYLEX", key, min, max]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return self.execute(command)

    def zrangebyscore(
        self,
        key: str,
        min: FloatMinMaxT,
        max: FloatMinMaxT,
        withscores: bool = False,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        """
        Returns the members of a sorted which have scores between a min and max value.

        Deprecated: use zrange with sortby="BYSCORE" instead.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zrangebyscore("myset", 1, 2) == ["a", "b"]

        assert redis.zrangebyscore("myset", "-inf", "+inf") == ["a", "b", "c"]
        ```

        See https://redis.io/commands/zrangebyscore
        """

        if number_are_not_none(offset, count, number=1):
            raise Exception('Both "offset" and "count" must be specified.')

        command: List = ["ZRANGEBYSCORE", key, min, max]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zrangestore(
        self,
        dst: str,
        src: str,
        min: FloatMinMaxT,
        max: FloatMinMaxT,
        sortby: Optional[Literal["BYSCORE", "BYLEX"]] = None,
        rev: bool = False,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        """
        Stores the result of a zrange command in a new sorted set.

        :param dst: the key of the new sorted set.
        :param src: the key of the source sorted set.
        :param min: the minimum value to include.
        :param max: the maximum value to include.
        :param sortby: whether to sort by score or lexicographically.
        :param rev: whether to reverse the results.
        :param offset: the offset to start from.
        :param count: the number of elements to return.

        Returns the number of elements in the new sorted set.

        Example:
        ```python
        redis.zadd("src", {"a": 1, "b": 2, "c": 3})

        assert redis.zrangestore("dest", "src", 1, 2) == 2
        ```

        See https://redis.io/commands/zrangestore
        """

        handle_non_deprecated_zrange_exceptions(sortby, min, max, offset, count)

        command: List = ["ZRANGESTORE", dst, src, min, max]

        if sortby:
            command.append(sortby)

        if rev:
            command.append("REV")

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return self.execute(command)

    def zrank(self, key: str, member: str) -> ResponseT:
        """
        Returns the rank of a member in a sorted set.

        The rank is 0-based, with the member with the lowest score being rank 0.

        If the member does not exist, None is returned.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zrank("myset", "a") == 0

        assert redis.zrank("myset", "d") is None

        assert redis.zrank("myset", "b") == 1

        assert redis.zrank("myset", "c") == 2
        ```

        See https://redis.io/commands/zrank
        """

        command: List = ["ZRANK", key, member]

        return self.execute(command)

    def zrem(self, key: str, *members: str) -> ResponseT:
        """
        Removes one or more members from a sorted set.

        Returns the number of members that were removed.

        Example:
        ```python
        redis.zadd("myset", {"one": 1, "two": 2, "three": 3})

        assert redis.zrem("myset", "one", "four") == 1
        ```

        See https://redis.io/commands/zrem
        """

        if len(members) == 0:
            raise Exception("At least one member must be removed.")

        command: List = ["ZREM", key, *members]

        return self.execute(command)

    def zremrangebylex(self, key: str, min: str, max: str) -> ResponseT:
        """
        Removes all members in a sorted set between a min and max value lexicographically.

        See https://redis.io/commands/zremrangebylex
        """

        if not min.startswith(("(", "[", "+", "-")) or not max.startswith(
            ("(", "[", "+", "-")
        ):
            raise Exception(
                "\"min\" and \"max\" must either start with '(' or '[' or be '+' or '-'."
            )

        command: List = ["ZREMRANGEBYLEX", key, min, max]

        return self.execute(command)

    def zremrangebyrank(self, key: str, start: int, stop: int) -> ResponseT:
        """
        Removes all members in a sorted set between a rank range.

        See https://redis.io/commands/zremrangebyrank
        """

        command: List = ["ZREMRANGEBYRANK", key, start, stop]

        return self.execute(command)

    def zremrangebyscore(
        self, key: str, min: FloatMinMaxT, max: FloatMinMaxT
    ) -> ResponseT:
        """
        Removes all members in a sorted set between a min and max score.

        See https://redis.io/commands/zremrangebyscore\

        If you need to use "-inf" and "+inf", please write them as strings.
        """

        command: List = ["ZREMRANGEBYSCORE", key, min, max]

        return self.execute(command)

    def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> ResponseT:
        """
        Returns the members of a sorted set between a min and max value in reverse order.

        Deprecated: use zrange with rev=True instead.

        See https://redis.io/commands/zrevrange
        """

        command: List = ["ZREVRANGE", key, start, stop]

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zrevrangebylex(
        self,
        key: str,
        max: str,
        min: str,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        """
        Returns the members of a sorted set between a min and max value ordered lexicographically in reverse order.

        Deprecated: use zrange with sortby="BYLEX" and rev=True instead.

        See https://redis.io/commands/zrevrangebylex
        """

        handle_zrangebylex_exceptions(min, max, offset, count)

        command: List = ["ZREVRANGEBYLEX", key, max, min]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        return self.execute(command)

    def zrevrangebyscore(
        self,
        key: str,
        max: FloatMinMaxT,
        min: FloatMinMaxT,
        withscores: bool = False,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        """
        Returns the members of a sorted set whose scores are between a min and max value in reverse order.

        Deprecated: use zrange with sortby="BYSCORE" and rev=True instead.

        See https://redis.io/commands/zrevrangebyscore
        """

        if number_are_not_none(offset, count, number=1):
            raise Exception('Both "offset" and "count" must be specified.')

        command: List = ["ZREVRANGEBYSCORE", key, max, min]

        if offset is not None:
            command.extend(["LIMIT", offset, count])

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zrevrank(self, key: str, member: str) -> ResponseT:
        """
        Returns the rank of a member in a sorted set in reverse order.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zrevrank("myset", "a") == 2
        ```

        See https://redis.io/commands/zrevrank
        """

        command: List = ["ZREVRANK", key, member]

        return self.execute(command)

    def zscan(
        self,
        key: str,
        cursor: int,
        match: Optional[str] = None,
        count: Optional[int] = None,
    ) -> ResponseT:
        """
        Returns a paginated list of members and their scores of an ordered set matching a pattern.

        :param cursor: the cursor to use for the scan, 0 to start a new scan.
        :param match: a pattern to match.
        :param count: the number of elements to return per page.

        :return: a tuple containing the new cursor and a list of (key, score) tuples.

        Example:
        ```python
        # Get all elements of an ordered set.

        cursor = 0
        results = []

        while True:
            cursor, keys = redis.zscan("myzset", cursor, match="*")

            results.extend(keys)
            if cursor == 0:
                break

        for key, score in results:
            print(key, score)
        ```

        See https://redis.io/commands/zscan
        """

        command: List = ["ZSCAN", key, str(cursor)]

        if match is not None:
            command.extend(["MATCH", match])

        if count is not None:
            command.extend(["COUNT", count])

        # The raw result is composed of the new cursor and the List of elements.
        return self.execute(command)

    def zscore(self, key: str, member: str) -> ResponseT:
        """
        Returns the score of a member in a sorted set.

        If the member does not exist, None is returned.

        Example:
        ```python
        redis.zadd("myset", {"a": 1, "b": 2, "c": 3})

        assert redis.zscore("myset", "a") == 1
        ```

        See https://redis.io/commands/zscore
        """

        command: List = ["ZSCORE", key, member]

        return self.execute(command)

    def zunion(
        self,
        keys: List[str],
        weights: Optional[List[float]] = None,
        aggregate: Optional[Literal["SUM", "MIN", "MAX"]] = None,
        withscores: bool = False,
    ) -> ResponseT:
        """
        Returns the union between multiple sorted sets.

        If aggregate is specified, the resulting scores are aggregated using the specified method.

        :param keys: the keys of the sorted sets.
        :param weights: the weights to apply to the sorted sets.
        :param aggregate: the method to use to aggregate scores.
        :param withscores: whether to return the scores along with the members.

        Returns a list of members or a list of (member, score) tuples if withscores is True.

        Example:
        ```python
        redis.zadd("key1", {"a": 1, "b": 2, "c": 3})

        redis.zadd("key2", {"c": 3, "d": 4, "e": 5})

        result = redis.zunion(["key1", "key2"])

        assert result == ["a", "b", "c", "d", "e"]
        ```

        See https://redis.io/commands/zunion
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZUNION", len(keys), *keys]

        if weights:
            command.extend(["WEIGHTS", *weights])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        if withscores:
            command.append("WITHSCORES")

        return self.execute(command)

    def zunionstore(
        self,
        destination: str,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Optional[Literal["SUM", "MIN", "MAX"]] = None,
    ) -> ResponseT:
        """
        Returns the union between multiple sorted sets.

        If aggregate is specified, the resulting scores are aggregated using the specified method.

        :param destination: the key of the sorted set to store the result in.
        :param keys: the keys of the sorted sets.
        :param weights: the weights to apply to the sorted sets.
        :param aggregate: the method to use to aggregate scores.
        :param withscores: whether to return the scores along with the members.

        Returns the number of elements in the resulting set.

        Example:
        ```python
        redis.zadd("key1", {"a": 1, "b": 2, "c": 3})

        redis.zadd("key2", {"c": 3, "d": 4, "e": 5})

        result = redis.unionstore("dest", ["key1", "key2"])

        assert result == 5
        ```

        See https://redis.io/commands/zunionstore
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["ZUNIONSTORE", destination, len(keys), *keys]

        if weights:
            command.extend(["WEIGHTS", *weights])

        if aggregate:
            command.extend(["AGGREGATE", aggregate])

        return self.execute(command)

    def append(self, key: str, value: str) -> ResponseT:
        """
        Appends a value to a string stored at a key.

        If the key does not exist, it is created.

        Returns the length of the string after the append operation.

        Example:
        ```python
        redis.set("key", "Hello")

        assert redis.append("key", " World") == 11

        assert redis.get("key") == "Hello World"
        ```

        See https://redis.io/commands/append
        """

        command: List = ["APPEND", key, value]

        return self.execute(command)

    def decr(self, key: str) -> ResponseT:
        """
        Decrements the integer value of a key by one.

        If the key does not exist, it is set to 0 before performing the operation.

        Returns the value of the key after the decrement operation.

        Example:
        ```
        redis.set("key", 10)

        assert redis.decr("key") == 9
        ```

        See https://redis.io/commands/decr
        """

        command: List = ["DECR", key]

        return self.execute(command)

    def decrby(self, key: str, decrement: int) -> ResponseT:
        """
        Decrements the integer value of a key by the given number.

        If the key does not exist, it is set to 0 before performing the operation.

        Returns the value of the key after the decrement operation.

        Example:
        ```
        redis.set("key", 10)

        assert redis.decrby("key", 5) == 5
        ```

        See https://redis.io/commands/decrby
        """

        command: List = ["DECRBY", key, decrement]

        return self.execute(command)

    def get(self, key: str) -> ResponseT:
        """
        Returns the value of a key or None if the key does not exist.

        Example:
        ```python
        redis.set("key", "value")

        assert redis.get("key") == "value"
        ```

        See https://redis.io/commands/get
        """

        command: List = ["GET", key]

        return self.execute(command)

    def getdel(self, key: str) -> ResponseT:
        """
        Gets the value of a key and delete the key atomically.

        Returns the value of the key or None if the key does not exist.

        Example:
        ```python
        redis.set("key", "value")

        assert redis.getdel("key") == "value"

        assert redis.get("key") is None
        ```

        See https://redis.io/commands/getdel
        """

        command: List = ["GETDEL", key]

        return self.execute(command)

    def getex(
        self,
        key: str,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        exat: Optional[int] = None,
        pxat: Optional[int] = None,
        persist: Optional[bool] = None,
    ) -> ResponseT:
        """
        Gets the value of a key and optionally set its expiration.

        Returns the value of the key or None if the key does not exist.

        :param ex: the number of seconds until the key expires.
        :param px: the number of milliseconds until the key expires.
        :param exat: the UNIX timestamp in seconds until the key expires.
        :param pxat: the UNIX timestamp in milliseconds until the key expires.
        :param persist: Remove the expiration from the key.

        Example:
        ```python
        redis.set("key", "value")

        assert redis.getex("key", ex=60) == "value"

        # The key will expire in 60 seconds.
        ```

        See https://redis.io/commands/getex
        """

        if (ex or px or exat or pxat or persist) and not number_are_not_none(
            ex, px, exat, pxat, persist, number=1
        ):
            raise Exception("Exactly one of the expiration settings must be specified.")

        command: List = ["GETEX", key]

        if ex is not None:
            command.extend(["EX", ex])

        if px is not None:
            command.extend(["PX", px])

        if exat is not None:
            command.extend(["EXAT", exat])

        if pxat is not None:
            command.extend(["PXAT", pxat])

        if persist is not None:
            command.append("PERSIST")

        return self.execute(command)

    def getrange(self, key: str, start: int, end: int) -> ResponseT:
        """
        Returns a substring of a string stored at a key.

        The substring is inclusive of the start and end indices.

        Negative indices can be used to specify offsets starting at the end of the string.

        Example:
        ```python
        redis.set("key", "Hello World")

        assert redis.getrange("key", 0, 4) == "Hello"
        ```

        See https://redis.io/commands/getrange
        """

        command: List = ["GETRANGE", key, start, end]

        return self.execute(command)

    def getset(self, key: str, value: ValueT) -> ResponseT:
        """
        Sets the value of a key and return its old value.

        If the key does not exist, None is returned.

        Example:
        ```python
        redis.set("key", "old-value")

        assert redis.getset("key", "newvalue") == "old-value"
        ```

        See https://redis.io/commands/getset
        """

        command: List = ["GETSET", key, value]

        return self.execute(command)

    def incr(self, key: str) -> ResponseT:
        """
        Increments the integer value of a key by one.

        If the key does not exist, it is set to 0 before performing the operation.

        Returns the value of the key after the increment operation.

        Example:
        ```python
        redis.set("key", 10)

        assert redis.incr("key") == 11
        ```

        See https://redis.io/commands/incr
        """

        command: List = ["INCR", key]

        return self.execute(command)

    def incrby(self, key: str, increment: int) -> ResponseT:
        """
        Increments the integer value of a key by the given number.

        If the key does not exist, it is set to 0 before performing the operation.

        Returns the value of the key after the increment operation.

        Example:
        ```python
        redis.set("key", 10)

        assert redis.incrby("key", 5) == 15
        ```

        See https://redis.io/commands/incrby
        """

        command: List = ["INCRBY", key, increment]

        return self.execute(command)

    def incrbyfloat(self, key: str, increment: float) -> ResponseT:
        """
        Increments the float value of a key by the given number.

        If the key does not exist, it is set to 0 before performing the operation.

        Returns the value of the key after the increment operation.

        Example:
        ```python
        redis.set("key", 10.50)

        # 10.60
        result = redis.incrbyfloat("key", 0.1)
        ```

        See https://redis.io/commands/incrbyfloat
        """

        command: List = ["INCRBYFLOAT", key, increment]

        return self.execute(command)

    def mget(self, *keys: str) -> ResponseT:
        """
        Returns the values of all the given keys.

        If a key does not exist, None is returned.

        Example:
        ```python
        redis.set("key1", "value1")

        redis.set("key2", "value2")

        assert redis.mget("key1", "key2") == ["value1", "value2"]
        ```

        See https://redis.io/commands/mget
        """

        if len(keys) == 0:
            raise Exception("At least one key must be specified.")

        command: List = ["MGET", *keys]

        return self.execute(command)

    def mset(self, values: Mapping[str, ValueT]) -> ResponseT:
        """
        Sets multiple keys to multiple values.

        Returns "OK" if all the keys were set.

        Example:
        ```python
        redis.mset({
            "key1": "value1",
            "key2": "value2"
        })
        ```

        See https://redis.io/commands/mset
        """

        command: List = ["MSET"]

        for key, value in values.items():
            command.extend([key, value])

        return self.execute(command)

    def msetnx(self, values: Mapping[str, ValueT]) -> ResponseT:
        """
        Sets multiple keys to multiple values, only if none of the keys exist.

        Returns `True` if all the keys were set, `False` otherwise.

        Example:
        ```python
        redis.msetnx({
            "key1": "value1",
            "key2": "value2"
        })
        ```
        See https://redis.io/commands/msetnx
        """

        command: List = ["MSETNX"]

        for key, value in values.items():
            command.extend([key, value])

        return self.execute(command)

    def psetex(self, key: str, milliseconds: int, value: ValueT) -> ResponseT:
        """
        Sets the value of a key and set its expiration in milliseconds.

        If the key does not exist, it is created.

        DEPRECATED: Use "set" with "px" option instead.

        Example:
        ```python
        # The key will expire in 1000 milliseconds.
        redis.psetex("key", 1000, "value")
        ```

        See https://redis.io/commands/psetex
        """

        command: List = ["PSETEX", key, milliseconds, value]

        return self.execute(command)

    def set(
        self,
        key: str,
        value: ValueT,
        nx: Optional[bool] = None,
        xx: Optional[bool] = None,
        get: Optional[bool] = None,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        exat: Optional[int] = None,
        pxat: Optional[int] = None,
        keepttl: Optional[bool] = None,
    ) -> ResponseT:
        """
        Sets the value of a key and optionally set its expiration.

        Returns `True` if the key was set.

        :param nx: Only set the key if it does not already exist.
        :param xx: Only set the key if it already exists.

        :param get: Return the old value stored at the key.

        :param ex: Set the number of seconds until the key expires.
        :param px: Set the number of milliseconds until the key expires.
        :param exat: Set the UNIX timestamp in seconds until the key expires.
        :param pxat: Set the UNIX timestamp in milliseconds until the key expires.

        :param keepttl: Don't reset the ttl of the key.

        Example:
        ```python
        assert redis.set("key", "value") is True

        assert redis.get("key") == "value"

        # Only set the key if it does not already exist.
        assert redis.set("key", "value", nx=True) is False

        # Only set the key if it already exists.
        assert redis.set("key", "value", xx=True) is True

        # Get the old value stored at the key.
        assert redis.set("key", "new-value", get=True) == "value"
        ```

        See https://redis.io/commands/set
        """

        if nx and xx:
            raise Exception('"nx" and "xx" are mutually exclusive.')

        if (ex or px or exat or pxat or keepttl) and not number_are_not_none(
            ex, px, exat, pxat, keepttl, number=1
        ):
            raise Exception("Exactly one of the expiration settings must be specified.")

        if nx and get:
            raise Exception('"nx" and "get" are mutually exclusive.')

        command: List = ["SET", key, value]

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        if get:
            command.append("GET")

        if ex is not None:
            command.extend(["EX", ex])

        if px is not None:
            command.extend(["PX", px])

        if exat is not None:
            command.extend(["EXAT", exat])

        if pxat is not None:
            command.extend(["PXAT", pxat])

        if keepttl:
            command.append("KEEPTTL")

        return self.execute(command)

    def setex(self, key: str, seconds: int, value: ValueT) -> ResponseT:
        """
        Sets the value of a key and set its expiration in seconds.

        Deprecated: Use "set" with "ex" option instead.

        Example:
        ```python
        # The key will expire in 60 seconds.
        redis.setex("key", 60, "value")
        ```

        See https://redis.io/commands/setex
        """

        command: List = ["SETEX", key, seconds, value]

        return self.execute(command)

    def setnx(self, key: str, value: ValueT) -> ResponseT:
        """
        Sets the value of a key, only if the key does not already exist.

        Returns `True` if the key was set.

        Deprecated: Use "set" with "nx" option instead.

        Example:
        ```python
        # The key does not exist, so it will be set.
        assert redis.setnx("key", "value") is True

        # The key already exists, so it will not be set.
        assert redis.setnx("key", "value") is False
        ```

        See https://redis.io/commands/setnx
        """

        command: List = ["SETNX", key, value]

        return self.execute(command)

    def setrange(self, key: str, offset: int, value: str) -> ResponseT:
        """
        Overwrites part of a string at key starting at the specified offset.

        If the offset is larger than the current length of the string at key,
        the string is padded with zero-bytes to make offset fit.

        Returns the length of the string after it was modified by the command.

        Example:
        ```python
        redis.set("key", "Hello World")

        assert redis.setrange("key", 6, "Redis") == 11

        assert redis.get("key") == "Hello Redis"
        ```

        See https://redis.io/commands/setrange
        """

        command: List = ["SETRANGE", key, offset, value]

        return self.execute(command)

    def strlen(self, key: str) -> ResponseT:
        """
        Returns the length of the string stored at a key.

        Example:
        ```python
        redis.set("key", "Hello World")

        assert redis.strlen("key") == 11
        ```

        See https://redis.io/commands/strlen
        """

        command: List = ["STRLEN", key]

        return self.execute(command)

    def substr(self, key: str, start: int, end: int) -> ResponseT:
        """
        Returns a substring of a string stored at a key.

        The substring is inclusive of the start and end indices.

        Negative indices can be used to specify offsets starting at the end of the string.

        Deprecated: Use "getrange" instead.

        Example:
        ```python
        redis.set("key", "Hello World")

        assert redis.substr("key", 0, 4) == "Hello"

        assert redis.substr("key", -5, -1) == "World"

        assert redis.substr("key", 6, -1) == "World"

        assert redis.substr("key", 6, 11) == "World"
        ```

        See https://redis.io/commands/substr
        """

        command: List = ["SUBSTR", key, start, end]

        return self.execute(command)

    def script_exists(self, *sha1: str) -> ResponseT:
        """
        Checks if the given sha1 digests exist in the script cache.

        Returns a list of booleans indicating which scripts exist.

        See https://redis.io/commands/script-exists
        """

        if len(sha1) == 0:
            raise Exception("At least one sha1 digests must be provided.")

        command: List = ["SCRIPT", "EXISTS", *sha1]

        return self.execute(command)

    def script_flush(
        self, flush_type: Optional[Literal["ASYNC", "SYNC"]] = None
    ) -> ResponseT:
        """
        Removes all the scripts from the script cache.

        Flush type can be "ASYNC" or "SYNC".

        See https://redis.io/commands/script-flush
        """

        command: List = ["SCRIPT", "FLUSH"]

        if flush_type:
            command.append(flush_type)

        return self.execute(command)

    def script_load(self, script: str) -> ResponseT:
        """
        Loads the given script into the script cache

        Returns the sha1 digest of the script.

        Example:
        ```python
        sha1 = redis.script_load("return 1")

        assert redis.evalsha(sha1) == 1
        ```

        See https://redis.io/commands/script-load
        """

        command: List = ["SCRIPT", "LOAD", script]

        return self.execute(command)


class JsonCommands:
    def __init__(self, client: Commands):
        self.client = client

    def arrappend(self, key: str, path: str = "$", *values: JSONValueT) -> ResponseT:
        """
        Appends one or more values to a JSON array stored at a key.

        Returns the length of the array after the append operation.

        See https://redis.io/commands/json.arrappend
        """
        command: List = ["JSON.ARRAPPEND", key, path]
        for value in values:
            if isinstance(value, str):
                command.append(f'"{value}"')
            else:
                command.append(value)

        return self.client.execute(command=command)

    def arrindex(
        self, key: str, path: str, value: JSONValueT, start: int = 0, stop: int = 0
    ) -> ResponseT:
        """
        Returns the index of the first occurrence of a value in a JSON array.

        If the value is not found, None is returned.

        See https://redis.io/commands/json.arrindex
        """
        if isinstance(value, str):
            value = f'"{value}"'

        command: List = ["JSON.ARRINDEX", key, path, value, start, stop]

        return self.client.execute(command=command)

    def arrinsert(
        self, key: str, path: str, index: int, *values: JSONValueT
    ) -> ResponseT:
        """
        Inserts one or more values to a JSON array stored at a key at a specified index.

        Returns the length of the array after the insert operation.

        See https://redis.io/commands/json.arrinsert
        """
        command: List = ["JSON.ARRINSERT", key, path, index]
        for value in values:
            if isinstance(value, str):
                command.append(f'"{value}"')
            else:
                command.append(value)

        return self.client.execute(command=command)

    def arrlen(self, key: str, path: str = "$") -> ResponseT:
        """
        Returns the length of a JSON array stored at a key.

        See https://redis.io/commands/json.arrlen
        """
        command: List = ["JSON.ARRLEN", key, path]

        return self.client.execute(command=command)

    def arrpop(self, key: str, path: str = "$", index: int = -1) -> ResponseT:
        """
        Removes and returns the element at the specified index from a JSON array stored at a key.

        If the index is negative, it is used as a negative index from the end of the array.

        See https://redis.io/commands/json.arrpop
        """
        command: List = ["JSON.ARRPOP", key, path, index]

        return self.client.execute(command=command)

    def arrtrim(self, key: str, path: str, start: int, stop: int) -> ResponseT:
        """
        Trims a JSON array stored at a key to the specified range of elements.

        Returns the length of the array after the trim operation.

        See https://redis.io/commands/json.arrtrim
        """
        command: List = ["JSON.ARRTRIM", key, path, start, stop]

        return self.client.execute(command=command)

    def clear(self, key: str, path: str = "$") -> ResponseT:
        """
        Sets the value at a specified path in a JSON document stored at a key to default value of the type.

        See https://redis.io/commands/json.clear
        """
        command: List = ["JSON.CLEAR", key, path]

        return self.client.execute(command=command)

    def delete(self, key: str, path: str = "$") -> ResponseT:
        """
        Removes the value at a specified path in a JSON document stored at a key.

        See https://redis.io/commands/json.del
        """
        command: List = ["JSON.DEL", key, path]

        return self.client.execute(command=command)

    def forget(self, key: str, path: str = "$") -> ResponseT:
        """
        Removes the value at a specified path in a JSON document stored at a key.

        See https://redis.io/commands/json.forget
        """
        command: List = ["JSON.FORGET", key, path]

        return self.client.execute(command=command)

    def get(self, key: str, *paths: str) -> ResponseT:
        """
        Returns the value at a specified path in a JSON document stored at a key.

        See https://redis.io/commands/json.get
        """
        command: List = ["JSON.GET", key]

        if len(paths) > 0:
            command.extend(paths)
        else:
            command.append("$")

        return self.client.execute(command=command)

    def merge(self, key: str, path: str, value: JSONValueT) -> ResponseT:
        """
        Merges the value at a specified path in a JSON document stored at a key.

        Returns true if the value was merged.

        See https://redis.io/commands/json.merge
        """
        if isinstance(value, str):
            value = f'"{value}"'

        command: List = ["JSON.MERGE", key, path, value]

        return self.client.execute(command=command)

    def mget(self, keys: List[str], path: str) -> ResponseT:
        """
        Returns the values at the specified paths in multiple JSON documents stored at multiple keys.

        See https://redis.io/commands/json.mget
        """

        command: List = ["JSON.MGET", *keys, path]

        return self.client.execute(command=command)

    def mset(
        self, key_path_value_tuples: List[Tuple[str, str, JSONValueT]]
    ) -> ResponseT:
        """
        Sets the values at specified paths in multiple JSON documents stored at multiple keys.

        Returns True if the values were set.

        See https://redis.io/commands/json.mset
        """
        command = ["JSON.MSET"]

        for key, path, value in key_path_value_tuples:
            if isinstance(value, str):
                value = f'"{value}"'

            command.extend([key, path, value])

        return self.client.execute(command=command)

    def numincrby(self, key: str, path: str, increment: int) -> ResponseT:
        """
        Increments the number value at a specified path in a JSON document stored at a key by a specified amount.

        Returns the value of the number after the increment operation.

        See https://redis.io/commands/json.numincrby
        """
        command: List = ["JSON.NUMINCRBY", key, path, increment]

        return self.client.execute(command=command)

    def nummultby(self, key: str, path: str, multiply: int) -> ResponseT:
        """
        Multiplies the number value at a specified path in a JSON document stored at a key by a specified amount.

        Returns the value of the number after the multiplication operation.

        See https://redis.io/commands/json.nummultby
        """
        command: List = ["JSON.NUMMULTBY", key, path, multiply]

        return self.client.execute(command=command)

    def objkeys(self, key: str, path: str = "$") -> ResponseT:
        """
        Returns the object keys in the object at a specified path in a JSON document stored at a key.

        See https://redis.io/commands/json.objkeys
        """
        command: List = ["JSON.OBJKEYS", key, path]

        return self.client.execute(command=command)

    def objlen(self, key: str, path: str = "$") -> ResponseT:
        """
        Returns the number of keys in the object at a specified path in a JSON document stored at a key.

        See https://redis.io/commands/json.objlen
        """
        command: List = ["JSON.OBJLEN", key, path]

        return self.client.execute(command=command)

    def resp(self, key: str, path: str = "$") -> ResponseT:
        """
        Returns the value at a specified path in redis serialization protocol format.

        See https://redis.io/commands/json.resp
        """
        command: List = ["JSON.RESP", key, path]

        return self.client.execute(command=command)

    def set(
        self,
        key: str,
        path: str,
        value: JSONValueT,
        nx: Optional[bool] = None,
        xx: Optional[bool] = None,
    ) -> ResponseT:
        """
        Sets the value at a specified path in a JSON document stored at a key.

        Returns True if the value was set.

        See https://redis.io/commands/json.set
        """
        if isinstance(value, str):
            value = f'"{value}"'

        command: List = ["JSON.SET", key, path, value]

        if nx:
            command.append("NX")
        if xx:
            command.append("XX")

        return self.client.execute(command=command)

    def strappend(self, key: str, path: str, value: str) -> ResponseT:
        """
        Appends a string to the string value at a specified path in a JSON document stored at a key.

        See https://redis.io/commands/json.strappend
        """
        command: List = ["JSON.STRAPPEND", key, path, f'"{value}"']

        return self.client.execute(command=command)

    def strlen(self, key: str, path: str = "$") -> ResponseT:
        """
        Returns the length of the string value at a specified path in a JSON document stored at a key.

        See https://redis.io/commands/json.strlen
        """
        command: List = ["JSON.STRLEN", key, path]

        return self.client.execute(command=command)

    def toggle(self, key: str, path: str = "$") -> ResponseT:
        """
        Toggles a boolean value at a specified path in a JSON document stored at a key.

        See https://redis.io/commands/json.toggle
        """
        command: List = ["JSON.TOGGLE", key, path]

        return self.client.execute(command=command)

    def type(self, key: str, path: str = "$") -> ResponseT:
        """
        Returns the type of the value at a specified path in a JSON document stored at a key.

        See https://redis.io/commands/json.type
        """
        command: List = ["JSON.TYPE", key, path]

        return self.client.execute(command=command)


# It doesn't inherit from "Redis" mainly because of the methods signatures.
class BitFieldCommands:
    def __init__(self, client: Commands, key: str):
        self.client = client
        self.command: List = ["BITFIELD", key]

    def get(self, encoding: str, offset: Union[int, str]) -> "BitFieldCommands":
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    def set(
        self, encoding: str, offset: Union[int, str], value: int
    ) -> "BitFieldCommands":
        """
        Set the specified bit field and returns its old value.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["SET", encoding, offset, value]
        self.command.extend(_command)

        return self

    def incrby(
        self, encoding: str, offset: Union[int, str], increment: int
    ) -> "BitFieldCommands":
        """
        Increments or decrements (if a negative increment is given) the specified bit field and returns the new value.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["INCRBY", encoding, offset, increment]
        self.command.extend(_command)

        return self

    def overflow(self, overflow: Literal["WRAP", "SAT", "FAIL"]) -> "BitFieldCommands":
        """
        Where an integer encoding is expected, it can be composed by prefixing with i
        for signed integers and u for unsigned integers with the number of bits of our integer encoding.
        So for example u8 is an unsigned integer of 8 bits and i16 is a signed integer of 16 bits.
        The supported encodings are up to 64 bits for signed integers, and up to 63 bits for unsigned integers.
        This limitation with unsigned integers is due to the fact that currently the Redis protocol is unable to
        return 64-bit unsigned integers as replies.

        Source: https://redis.io/commands/bitfield
        """

        _command = ["OVERFLOW", overflow]
        self.command.extend(_command)

        return self

    def execute(self) -> ResponseT:
        return self.client.execute(command=self.command)


class BitFieldROCommands:
    def __init__(self, client: Commands, key: str):
        self.client = client
        self.command: List = ["BITFIELD_RO", key]

    def get(self, encoding: str, offset: Union[int, str]) -> "BitFieldROCommands":
        """
        Returns the specified bit field.

        Source: https://redis.io/commands/bitfield_ro
        """

        _command = ["GET", encoding, offset]
        self.command.extend(_command)

        return self

    def execute(self) -> ResponseT:
        return self.client.execute(command=self.command)


AsyncCommands = Commands
AsyncJsonCommands = JsonCommands
AsyncBitFieldCommands = BitFieldCommands
AsyncBitFieldROCommands = BitFieldROCommands
PipelineCommands = Commands
PipelineJsonCommands = JsonCommands
