
from upstash_redis.schema.commands.returns import (
    GeoMembersReturn,
    FormattedGeoMembersReturn,
)

from upstash_redis.schema.commands.parameters import (
    BitFieldOffset,
    GeoMember,
    FloatMinMax,
)

from typing import Any, Iterable, Optional, Set, Tuple, Union, List, Literal, Dict

class AsyncCommands:
    def __init__(self):
        ...

    async def bitcount(
        self, key: str, start: Union[int, None] = None, end: Union[int, None] = None
    ) -> int: 
        ...

    def bitfield(self, key: str) -> "BitFieldCommands":
        ...

    def bitfield_ro(self, key: str) -> "BitFieldRO":
        ...

    async def bitop(
        self, operation: Literal["AND", "OR", "XOR", "NOT"], destkey: str, *srckeys: str
    ) -> int:
        ...

    async def bitpos(
        self,
        key: str,
        bit: Literal[0, 1],
        start: Union[int, None] = None,
        end: Union[int, None] = None,
    ) -> int:
        ...

    async def getbit(self, key: str, offset: int) -> int:
        ...

    async def setbit(self, key: str, offset: int, value: Literal[0, 1]) -> int:
        ...

    async def ping(self, message: Union[str, None] = None) -> str:
        ...

    async def echo(self, message: str) -> str:
        ...
    
    async def copy(
        self, source: str, destination: str, replace: bool = False
    ) -> Union[Literal[1, 0], bool]:
        ...

    async def delete(self, *keys: str) -> int:
        ...

    async def exists(self, *keys: str) -> int:
        ...

    async def expire(self, key: str, seconds: int) -> Union[Literal[1, 0], bool]:
        ...

    async def expireat(
        self, key: str, unix_time_seconds: int
    ) -> Union[Literal[1, 0], bool]:
       ...

    async def keys(self, pattern: str) -> List[str]:
        ...

    async def persist(self, key: str) -> Union[Literal[1, 0], bool]:
        ...

    async def pexpire(self, key: str, milliseconds: int) -> Union[Literal[1, 0], bool]:
        ...

    async def pexpireat(
        self, key: str, unix_time_milliseconds: int
    ) -> Union[Literal[1, 0], bool]:
        ...

    async def pttl(self, key: str) -> int:
        ...

    async def randomkey(self) -> Union[str, None]:
        ...

    async def rename(self, key: str, newkey: str) -> str:
        ...

    async def renamenx(self, key: str, newkey: str) -> Union[Literal[1, 0], bool]:
        ...

    async def scan(
        self,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
        scan_type: Union[str, None] = None,
    ) -> Union[List[Union[str, List[str]]], List[Union[int, List[str]]]]:
        ...
    
    async def touch(self, *keys: str) -> int:
        ...

    async def ttl(self, key: str) -> int:
        ...

    async def type(self, key: str) -> Union[str, None]:
        ...

    async def unlink(self, *keys: str) -> int:
        ...

    async def geoadd(
        self,
        key: str,
        *members: GeoMember,
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
    ) -> int:
        ...

    async def geodist(
        self,
        key: str,
        member1: str,
        member2: str,
        unit: Literal["m", "km", "ft", "mi", "M", "KM", "FT", "MI"] = "M",
    ) -> Union[str, float, None]:
        ...

    async def geohash(self, key: str, *members: str) -> List[Union[str, None]]:
        ...

    async def geopos(
        self, key: str, *members: str
    ) -> Union[List[Union[List[str], None]], List[Union[Dict[str, float], None]]]:
        ...

    async def georadius(
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

    async def georadius_ro(
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

    async def georadiusbymember(
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

    async def georadiusbymember_ro(
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

    async def geosearch(
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

    async def geosearchstore(
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

    async def hdel(self, key: str, *fields: str) -> int:
        ...

    async def hexists(self, key: str, field: str) -> Union[Literal[1, 0], bool]:
        ...

    async def hget(self, key: str, field: str) -> Union[str, None]:
        ...

    async def hgetall(self, key: str) -> Union[List[str], Dict[str, str]]:
        ...

    async def hincrby(self, key: str, field: str, increment: int) -> int:
        ...

    async def hincrbyfloat(
        self, key: str, field: str, increment: float
    ) -> Union[str, float]:
        ...

    async def hkeys(self, key: str) -> List[str]:
        ...

    async def hlen(self, key: str) -> int:
        ...

    async def hmget(self, key: str, *fields: str) -> List[Union[str, None]]:
        ...

    async def hmset(self, key: str, field_value_pairs: Dict) -> str:
        ...

    async def hrandfield(
        self, key: str, count: Union[int, None] = None, withvalues: bool = False
    ):
        ...

    async def hscan(
        self,
        name: str,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> Tuple[int, Dict[str, str]]:
        ...

    async def hset(self, name: str, key: Optional[str] = None, val: Optional[str] = None, field_value_pairs: Optional[Dict] = None) -> int:
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

    async def pfcount(self, *keys: str) -> int:
        ...

    async def pfmerge(self, destkey: str, *sourcekeys: str) -> str:
        ...

    async def lindex(self, key: str, index: int) -> Union[str, None]:
        ...

    async def linsert(
        self, key: str, position: Literal["BEFORE", "AFTER", "before", "after"], pivot: Any, element: Any
    ) -> int:
       ...

    async def llen(self, key: str) -> int:
        ...

    async def lmove(
        self,
        source: str,
        destination: str,
        source_position: Literal["LEFT", "RIGHT"] = "LEFT",
        destination_position: Literal["LEFT", "RIGHT"] = "RIGHT",
    ) -> Union[str, None]:
        ...

    async def lpop(
        self, key: str, count: Union[int, None] = None
    ) -> Union[(Union[str, None]), List[str]]:
        ...

    async def lpos(
        self,
        key: str,
        element: Any,
        rank: Union[int, None] = None,
        count: Union[int, None] = None,
        maxlen: Union[int, None] = None,
    ) -> Union[(Union[int, None]), List[int]]:
        ...

    async def lpush(self, key: str, *elements: Any) -> int:
        ...

    async def lpushx(self, key: str, *elements: Any) -> int:
        ...

    async def lrange(self, key: str, start: int, stop: int) -> List[str]:
        ...

    async def lrem(self, key: str, count: int, element: Any) -> int:
        ...

    async def lset(self, key: str, index: int, element: Any) -> str:
        ...

    async def ltrim(self, key: str, start: int, stop: int) -> str:
        ...

    async def rpop(
        self, key: str, count: Union[int, None] = None
    ) -> Union[(Union[str, None]), List[str]]:
        ...

    async def rpoplpush(self, source: str, destination: str) -> Union[str, None]:
        ...

    async def rpush(self, key: str, *elements: Any) -> int:
        ...

    async def rpushx(self, key: str, *elements: Any) -> int:
        ...

    async def publish(self, channel: str, message: str) -> int:
        ...

    async def eval(
        self,
        script: str,
        keys: Union[List[str], None] = None,
        args: Union[List, None] = None,
    ) -> Any:
        ...

    async def evalsha(
        self,
        sha1: str,
        keys: Union[List[str], None] = None,
        args: Union[List, None] = None,
    ) -> Any:
        ...

    async def dbsize(self) -> int:
        ...

    async def flushall(self, mode: Union[Literal["ASYNC", "SYNC"], None] = None) -> Union[str, bool]:
        ...

    async def flushdb(self, mode: Union[Literal["ASYNC", "SYNC"], None] = None) -> Union[str, bool]:
        ...

    async def time(self) -> Union[List[str], Dict[str, int]]:
        ...

    async def sadd(self, key: str, *members: Any) -> int:
        ...

    async def scard(self, key: str) -> int:
        ...

    async def sdiff(self, *keys: str) -> Set[str]:
        ...

    async def sdiffstore(self, destination: str, *keys: str) -> int:
        ...

    async def sinter(self, *keys: str) -> Set[str]:
        ...

    async def sinterstore(self, destination: str, *keys: str) -> int:
        ...

    async def sismember(self, key: str, member: Any) -> Union[Literal[1, 0], bool]:
        ...

    async def smembers(self, key: str) -> Set[str]:
        ...

    async def smove(
        self, source: str, destination: str, member: Any
    ) -> Union[Literal[1, 0], bool]:
        ...

    async def spop(
        self, key: str, count: Union[int, None] = None
    ) -> Union[(Union[str, None]), List[str]]:
        ...

    async def srandmember(
        self, key: str, count: Union[int, None] = None
    ) -> Union[(Union[str, None]), List[str]]:
        ...

    async def srem(self, key: str, *members: Any) -> int:
        ...

    async def sscan(
        self,
        key: str,
        cursor: int = 0,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> Tuple[int, List[str]]:
        ...

    async def sunion(self, *keys: str) -> Set[str]:
        ...

    async def sunionstore(self, destination: str, *keys: str) -> int:
        ...

    async def zadd(
        self,
        key: str,
        score_member_pairs: Dict,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
        incr: bool = False,
    ) -> Union[int, (Union[str, None, float])]:
        ...
    
    async def zcard(self, key: str) -> int:
        ...

    async def zcount(
        self, key: str, min_score: FloatMinMax, max_score: FloatMinMax
    ) -> int:
        ...

    def zdiff(
        self, keys: List[str], withscores: bool = False
    ) -> Union[List[str], List[Tuple[str, float]]]:
        ...

    async def zdiffstore(self, destination: str, keys: List[str]) -> int:
        ...

    async def zincrby(
        self, key: str, increment: float, member: str
    ) -> Union[str, float]:
        ...

    async def zinter(
        self,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
        withscores: bool = False,
    ) -> Union[List[str], List[Tuple[str, float]]]:
        ...

    async def zinterstore(
        self,
        destination: str,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
    ) -> int:
        ...

    async def zlexcount(self, key: str, min_score: str, max_score: str) -> int:
        ...

    async def zmscore(
        self, key: str, members: List[str]
    ) -> Union[List[Union[str, None]], List[Union[float, None]]]:
        ...

    async def zpopmax(
        self, key: str, count: Union[int, None] = None
    ) -> Union[List[str], List[Tuple[str, float]]]:
        ...

    async def zpopmin(
        self, key: str, count: Union[int, None] = None
    ) -> Union[List[str], List[Tuple[str, float]]]:
        ...

    async def zrandmember(
        self, key: str, count: Union[int, None] = None, withscores: bool = False
    ) -> Union[(Union[str, None]), (Union[List[str], List[Tuple[str, float]]])]:
        ...

    async def zrange(
        self,
        key: str,
        start: FloatMinMax,
        stop: FloatMinMax,
        range_method: Union[Literal["BYSCORE", "BYLEX"], None] = None,
        rev: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
        withscores: bool = False,
    ) -> Union[List[str], List[Tuple[str, float]]]:
        ...

    async def zrangebylex(
        self,
        key: str,
        min_score: str,
        max_score: str,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> List[Union[str, None]]:
        ...

    async def zrangebyscore(
        self,
        key: str,
        min_score: FloatMinMax,
        max_score: FloatMinMax,
        withscores: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> Union[List[str], List[Tuple[str, float]]]:
        ...

    async def zrangestore(
        self,
        dst: str,
        src: str,
        start: FloatMinMax,
        stop: FloatMinMax,
        range_method: Union[Literal["BYSCORE", "BYLEX"], None] = None,
        rev: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> int:
        ...

    async def zrank(self, key: str, member: str) -> Union[int, None]:
        ...

    async def zrem(self, key: str, *members: str) -> int:
        ...

    async def zremrangebylex(self, key: str, min_score: str, max_score: str) -> int:
        ...

    async def zremrangebyrank(self, key: str, start: int, stop: int) -> int:
        ...

    async def zremrangebyscore(
        self, key: str, min_score: FloatMinMax, max_score: FloatMinMax
    ) -> int:
        ...

    async def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> Union[List[str], List[Tuple[str, float]]]:
        ...

    async def zrevrangebylex(
        self,
        key: str,
        max_score: str,
        min_score: str,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> List[str]:
        ...

    async def zrevrangebyscore(
        self,
        key: str,
        max_score: FloatMinMax,
        min_score: FloatMinMax,
        withscores: bool = False,
        limit_offset: Union[int, None] = None,
        limit_count: Union[int, None] = None,
    ) -> Union[List[str], List[Tuple[str, float]]]:
        ...

    async def zrevrank(self, key: str, member: str) -> Union[int, None]:
        ...

    async def zscan(
        self,
        key: str,
        cursor: int,
        match_pattern: Union[str, None] = None,
        count: Union[int, None] = None,
    ) -> Tuple[int, List[Tuple[str, float]]]:
        ...

    async def zscore(self, key: str, member: str) -> Union[str, None, float]:
        ...

    def zunion(
        self,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
        withscores: bool = False,
    ) -> Union[List[str], List[Tuple[str, float]]]:
        ...

    async def zunionstore(
        self,
        destination: str,
        keys: List[str],
        weights: Union[List[float], List[int], None] = None,
        aggregate: Union[Literal["SUM", "MIN", "MAX"], None] = None,
    ) -> int:
        ...

    async def append(self, key: str, value: Any) -> int:
        ...

    async def decr(self, key: str) -> int:
        ...

    async def decrby(self, key: str, decrement: int) -> int:
        ...

    async def get(self, key: str) -> Union[str, None]:
        ...

    async def getdel(self, key: str) -> Union[str, None]:
        ...

    async def getex(
        self,
        key: str,
        ex: Union[int, None] = None,
        px: Union[int, None] = None,
        exat: Union[int, None] = None,
        pxat: Union[int, None] = None,
        persist: Union[bool, None] = None,
    ) -> Union[str, None]:
        ...

    async def getrange(self, key: str, start: int, end: int) -> str:
        ...

    async def getset(self, key: str, value: Any) -> Union[str, None]:
        ...

    async def incr(self, key: str) -> int:
        ...

    async def incrby(self, key: str, increment: int) -> int:
        ...

    async def incrbyfloat(self, key: str, increment: float) -> Union[str, float]:
        ...

    async def mget(self, *keys: str) -> List[Union[str, None]]:
        ...

    async def mset(self, key_value_pairs: Dict) -> Literal["OK"]:
        ...

    async def msetnx(self, key_value_pairs: Dict) -> Literal[1, 0]:
        ...

    async def psetex(self, key: str, milliseconds: int, value: Any) -> str:
        ...

    async def set(
        self,
        key: str,
        value: Any,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ex: Union[int, None] = None,
        px: Union[int, None] = None,
        exat: Union[int, None] = None,
        pxat: Union[int, None] = None,
        keepttl: bool = False,
    ) -> Union[str, None]:
        ...

    async def setex(self, key: str, seconds: int, value: Any) -> str:
        ...

    async def setnx(self, key: str, value: Any) -> Literal[1, 0]:
        ...
    
    async def setrange(self, key: str, offset: int, value: Any) -> int:
        ...

    async def strlen(self, key: str) -> int:
        ...

    async def substr(self, key: str, start: int, end: int) -> str:
        ...
    
    def pubsub(self) -> "PubSub":
        ...

    def script(self) -> "Script":
        ...


# It doesn't inherit from "Redis" mainly because of the methods signatures.
class BitFieldCommands:
    def __init__(self, client: AsyncCommands, key: str):
        ...

    def get(self, encoding: str, offset: BitFieldOffset) -> "BitFieldCommands":
        ...

    def set(self, encoding: str, offset: BitFieldOffset, value: int) -> "BitFieldCommands":
        ...

    def incrby(self, encoding: str, offset: BitFieldOffset, increment: int) -> "BitFieldCommands":
        ...

    def overflow(self, overflow: Literal["WRAP", "SAT", "FAIL"]) -> "BitFieldCommands":
        ...

    async def execute(self) -> List:
        ...


class BitFieldRO:
    def __init__(self, client: AsyncCommands, key: str):
        ...

    def get(self, encoding: str, offset: BitFieldOffset) -> "BitFieldRO":
        ...

    async def execute(self) -> List:
        ...


class PubSub:
    def __init__(self, client: AsyncCommands):
        ...

    async def channels(self, pattern: Union[str, None] = None) -> List[str]:
        ...

    async def numpat(self) -> int:
        ...

    async def numsub(
        self, *channels: str
    ) -> Union[List[Union[str, int]], Dict[str, int]]:
        ...


class Script:
    def __init__(self, client: AsyncCommands):
        ...

    async def exists(self, *sha1: str) -> Union[List[Literal[1, 0]], List[bool]]:
        ...

    async def flush(self, mode: Literal["ASYNC", "SYNC"]) -> str:
        ...

    async def load(self, script: str) -> str:
        ...