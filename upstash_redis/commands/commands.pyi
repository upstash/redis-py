
from upstash_redis.typing import CommandsProtocol, ResponseType
from upstash_redis.schema.commands.returns import (
    GeoMembersReturn,
    FormattedGeoMembersReturn,
    HashReturn,
    FormattedHashReturn,
    SortedSetReturn,
    FormattedSortedSetReturn,
)

from typing import Any, Union, List, Awaitable, Literal, Dict

class BasicKeyCommands:
    def bitcount(
        self, key: str, start: Union[int, None] = None, end: Union[int, None] = None
    ) -> int: 
        ...

    def bitfield(self, key: str) -> "BitFieldCommands":
        ...

    def bitfield_ro(self, key: str) -> "BitFieldRO":
        ...

    def bitop(
        self, operation: Literal["AND", "OR", "XOR", "NOT"], destkey: str, *srckeys: str
    ) -> int:
        ...

    def bitpos(
        self,
        key: str,
        bit: Literal[0, 1],
        start: Union[int, None] = None,
        end: Union[int, None] = None,
    ) -> int:
        ...

    def getbit(self, key: str, offset: int) -> int:
        ...

    def setbit(self, key: str, offset: int, value: Literal[0, 1]) -> int:
        ...

    def ping(self, message: Union[str, None] = None) -> str:
        ...

    def echo(self, message: str) -> str:
        ...
    
    def copy(
        self, source: str, destination: str, replace: bool = False
    ) -> Union[Literal[1, 0], bool]:
        ...

    def delete(self, *keys: str) -> int:
        ...

    def exists(self, *keys: str) -> int:
        ...

    def expire(self, key: str, seconds: int) -> Union[Literal[1, 0], bool]:
        ...

    def expireat(
        self, key: str, unix_time_seconds: int
    ) -> Union[Literal[1, 0], bool]:
       ...

    def keys(self, pattern: str) -> List[str]:
        ...

    def persist(self, key: str) -> Union[Literal[1, 0], bool]:
        ...

    def pexpire(self, key: str, milliseconds: int) -> Union[Literal[1, 0], bool]:
        ...

    def pexpireat(
        self, key: str, unix_time_milliseconds: int
    ) -> Union[Literal[1, 0], bool]:
        ...

    def pttl(self, key: str) -> int:
        ...

    def randomkey(self) -> Union[str, None]:
        ...

    def rename(self, key: str, newkey: str) -> str:
        ...

    def renamenx(self, key: str, newkey: str) -> Union[Literal[1, 0], bool]:
        ...

    def scan(
        self,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
        scan_type: Union[str, None] = None,
        return_cursor: bool = True,
    ) -> Union[
        (Union[List[Union[str, List[str]]], List[Union[int, List[str]]]]), List[str]
    ]:
        ...
    
    def touch(self, *keys: str) -> int:
        ...

    def ttl(self, key: str) -> int:
        ...

    def type(self, key: str) -> Union[str, None]:
        ...

    def unlink(self, *keys: str) -> int:
        ...

    def geoadd(
        self,
        key: str,
        *members: GeoMember,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> int:
        ...

    def geodist(
        self,
        key: str,
        member1: str,
        member2: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"] = "M",
    ) -> Union[str, float, None]:
        ...

    def geohash(self, key: str, *members: str) -> List[Union[str, None]]:
        ...

    def geopos(
        self, key: str, *members: str
    ) -> Union[List[Union[List[str], None]], List[Union[Dict[str, float], None]]]:
        ...

    def georadius(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        count_any: bool = False,
        sort: Union[Literal["ASC", "DESC"], None] = None,
        store: Union[str, None] = None,
        storedist: Union[str, None] = None,
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn, int]:
        ...

    def georadius_ro(
        self,
        key: str,
        longitude: float,
        latitude: float,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        count_any: bool = False,
        sort: Union[Literal["ASC", "DESC"], None] = None,
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn]:
        ...

    def georadiusbymember(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        count_any: bool = False,
        sort: Union[Literal["ASC", "DESC"], None] = None,
        store: Union[str, None] = None,
        storedist: Union[str, None] = None,
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn]:
        ...

    def georadiusbymember_ro(
        self,
        key: str,
        member: str,
        radius: float,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
        count: Union[int, None] = None,
        count_any: bool = False,
        sort: Union[Literal["ASC", "DESC"], None] = None,
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn]:
        ...

    def geosearch(
        self,
        key: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        frommember: Union[str, None] = None,
        fromlonlat_longitude: Union[float, None] = None,
        fromlonlat_latitude: Union[float, None] = None,
        byradius: Union[float, None] = None,
        bybox_width: Union[float, None] = None,
        bybox_height: Union[float, None] = None,
        sort: Union[Literal["ASC", "DESC"], None] = None,
        count: Union[int, None] = None,
        count_any: bool = False,
        withdist: bool = False,
        withhash: bool = False,
        withcoord: bool = False,
    ) -> Union[GeoMembersReturn, FormattedGeoMembersReturn]:
        ...

    def geosearchstore(
        self,
        destination: str,
        source: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"],
        frommember: Union[str, None] = None,
        fromlonlat_longitude: Union[float, None] = None,
        fromlonlat_latitude: Union[float, None] = None,
        byradius: Union[float, None] = None,
        bybox_width: Union[float, None] = None,
        bybox_height: Union[float, None] = None,
        sort: Union[Literal["ASC", "DESC"], None] = None,
        count: Union[int, None] = None,
        count_any: bool = False,
        storedist: bool = False,
    ) -> int:
        ...

    def hdel(self, key: str, *fields: str) -> int:
        ...

    def hexists(self, key: str, field: str) -> Union[Literal[1, 0], bool]:
        ...

    def hget(self, key: str, field: str) -> Union[str, None]:
        ...

    def hgetall(self, key: str) -> Union[HashReturn, FormattedHashReturn]:
        ...

    def hincrby(self, key: str, field: str, increment: int) -> int:
        ...

    def hincrbyfloat(
        self, key: str, field: str, increment: float
    ) -> Union[str, float]:
        ...

    def hkeys(self, key: str) -> List[str]:
        ...

    def hlen(self, key: str) -> int:
        ...

    def hmget(self, key: str, *fields: str) -> List[Union[str, None]]:
        ...

    def hmset(self, key: str, field_value_pairs: Dict) -> str:
        ...

    def hrandfield(
        self, key: str, count: Union[int, None] = None, withvalues: bool = False
    ) -> Union[(Union[str, None]), Union[HashReturn, FormattedHashReturn]]:
        ...

    def hscan(
        self,
        key: str,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
        return_cursor: bool = True,
    ) -> Union[
        (Union[List[Union[str, HashReturn]], List[Union[int, FormattedHashReturn]]]),
        (Union[HashReturn, FormattedHashReturn]),
    ]:
        ...

    def hset(self, key: str, field_value_pairs: Dict) -> int:
        ...

    async def hsetnx(
        self, key: str, field: str, value: Any
    ) -> Union[Literal[1, 0], bool]:
        ...

    async def hstrlen(self, key: str, field: str) -> int:
        ...

    async def hvals(self, key: str) -> List[str]:
        ...

    async def pfadd(self, key: str, *elements: Any) -> Union[Literal[1, 0], bool]:
        ...