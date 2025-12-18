import time
import uuid
from typing import Any, List, Optional, Tuple

from ddtrace.internal.logger import get_logger
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.config import Config
from redis import Redis
from redis.cluster import RedisCluster
from redis.sentinel import Sentinel

logger = get_logger('cache_client')

SECOND = 1
MINUTE = SECOND * 60
FIVE_MINUTE = MINUTE * 5
TEN_MINUTE = MINUTE * 10
THIRTY_MINUTE = MINUTE * 30
HOUR = MINUTE * 60
DAY = HOUR * 24
WEEK = DAY * 7


class CacheClient:
    client: Redis

    def __init__(self) -> None:
        self.initialized = False

    def initialize(self, config: Config):
        """
        Initialize Redis client. Supports standalone, cluster, and sentinel modes.
        """
        cfg_servers = config.get_str_list('OSPREY_REDIS_SERVERS', [])
        mode = config.get_str('OSPREY_REDIS_MODE', 'standalone')
        password = config.get_optional_str('OSPREY_REDIS_PASSWORD')
        db = config.get_int('OSPREY_REDIS_DB', 0)

        servers: List[Tuple[str, int]] = []
        for server_str in cfg_servers:
            pts = server_str.split(':')
            if len(pts) != 2:
                logger.error(f'Invalid server {server_str} included in OSPREY_REDIS_SERVERS')
                continue
            servers.append((pts[0], int(pts[1])))

        if not servers:
            logger.error('No valid Redis servers configured')
            return

        try:
            if mode == 'cluster':
                startup_nodes = [{'host': host, 'port': port} for host, port in servers]
                self.client = RedisCluster(
                    startup_nodes=startup_nodes,
                    password=password,
                    decode_responses=False,
                )
            elif mode == 'sentinel':
                sentinel_master = config.get_str('OSPREY_REDIS_SENTINEL_MASTER', 'mymaster')
                sentinel = Sentinel(servers, password=password)
                self.client = sentinel.master_for(
                    sentinel_master,
                    password=password,
                    db=db,
                    decode_responses=False,
                )
            else:
                host, port = servers[0]
                self.client = Redis(
                    host=host,
                    port=port,
                    password=password,
                    db=db,
                    decode_responses=False,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                )

            self.client.ping()
            self.initialized = True
            logger.info(f'Redis client initialized in {mode} mode')

        except Exception as e:
            logger.error(f'Failed to initialize Redis client: {e}')
            self.initialized = False

    def set(self, key: str, val: Any, ttl_seconds: float = 0):
        """
        Sets a value inside of the cache.
        """
        if not self.initialized:
            return

        try:
            if ttl_seconds > 0:
                self.client.setex(key, int(ttl_seconds), val)
            else:
                self.client.set(key, val)
        except Exception as e:
            logger.error(f'Error setting value in cache: {e}')

    def get_str(self, key: str, default: str = '') -> str:
        """
        Returns string value.
        """
        if not self.initialized:
            return default

        try:
            val = self.client.get(key)
            if val is None:
                return default
            if isinstance(val, bytes):
                return val.decode('utf-8')
            return str(val)
        except Exception as e:
            logger.error(f'Error getting string value in cache: {e}')
            return default

    def get_int(self, key: str, default: int = 0) -> int:
        """
        Returns int value.
        """
        if not self.initialized:
            return default

        try:
            val = self.client.get(key)
            if val is None:
                return default
            if isinstance(val, bytes):
                val = val.decode('utf-8')
            str_val = val.decode('utf-8') if isinstance(val, bytes) else str(val)
            return int(str_val)
        except Exception as e:
            logger.error(f'Error getting int value in cache: {e}')
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """
        Returns float value.
        """
        if not self.initialized:
            return default

        try:
            val = self.client.get(key)
            if val is None:
                return default
            if isinstance(val, bytes):
                val = val.decode('utf-8')
            str_val = val.decode('utf-8') if isinstance(val, bytes) else str(val)
            return float(str_val)
        except Exception as e:
            logger.error(f'Error getting float value in cache: {e}')
            return default

    def increment_window(
        self,
        key: str,
        window_seconds: float,
        max_ttl_seconds: Optional[float] = None,
        max_events_cap: int = 10_000,
    ) -> int:
        """
        Increment a sliding-window counter using Redis sorted sets.

        Each event is stored as a member with its timestamp as the score.
        This gives precise sliding window semantics.

        Returns the count within the current window on success, else 0.
        """
        if not self.initialized:
            return 0

        current_time = time.time()
        window_start = current_time - window_seconds

        member = f'{current_time}:{uuid.uuid4().hex[:8]}'

        if max_ttl_seconds is None:
            max_ttl_seconds = window_seconds * 2
        expire_seconds = int(max(window_seconds + 60, min(max_ttl_seconds, window_seconds * 2)))

        try:
            pipe = self.client.pipeline()
            pipe.zadd(key, {member: current_time})
            pipe.zremrangebyscore(key, '-inf', window_start)
            pipe.zcard(key)

            pipe.expire(key, expire_seconds)

            results = pipe.execute()
            count = results[2]

            if max_events_cap and count > max_events_cap:
                self.client.zremrangebyrank(key, 0, -(max_events_cap + 1))
                count = max_events_cap

            return count

        except Exception as e:
            logger.error(f'Redis sorted set increment failed for {key}: {e}')
            return 0

    def get_window_count(self, key: str, window_seconds: float) -> int:
        """
        Get the current count within the sliding window without incrementing.

        Returns the count within the current window on success, else 0.
        """
        if not self.initialized:
            return 0

        current_time = time.time()
        window_start = current_time - window_seconds

        try:
            count: int = self.client.zcount(key, window_start, '+inf')  # type: ignore[assignment]
            return count

        except Exception as e:
            logger.error(f'Redis zcount failed for {key}: {e}')
            return 0


cache_client = CacheClient()


class CacheArgumentsBase(ArgumentsBase):
    key: str


class CacheWindowArgumentsBase(CacheArgumentsBase):
    window_seconds: float
    when_all: List[bool]


class IncrementWindowArguments(CacheWindowArgumentsBase):
    max_ttl_seconds: Optional[float] = None


class CacheSetStrArguments(CacheArgumentsBase):
    value: str
    when_all: List[bool]
    ttl_seconds: float = DAY


class CacheSetIntArguments(CacheArgumentsBase):
    value: int
    when_all: List[bool]
    ttl_seconds: float = DAY


class CacheSetFloatArguments(CacheArgumentsBase):
    value: float
    when_all: List[bool]
    ttl_seconds: float = DAY


class CacheGetStrArguments(CacheArgumentsBase):
    when_all: List[bool]
    default: str = ''


class CacheGetIntArguments(CacheArgumentsBase):
    when_all: List[bool]
    default: int = 0


class CacheGetFloatArguments(CacheArgumentsBase):
    when_all: List[bool]
    default: float = 0.0


class CacheSetStr(UDFBase[CacheSetStrArguments, None]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: CacheSetStrArguments):
        if all(arguments.when_all) is not True:
            return
        cache_client.set(arguments.key, arguments.value, arguments.ttl_seconds)


class CacheSetInt(UDFBase[CacheSetIntArguments, None]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: CacheSetIntArguments):
        if all(arguments.when_all) is not True:
            return
        cache_client.set(arguments.key, arguments.value, arguments.ttl_seconds)


class CacheSetFloat(UDFBase[CacheSetFloatArguments, None]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: CacheSetFloatArguments):
        if all(arguments.when_all) is not True:
            return
        cache_client.set(arguments.key, arguments.value, arguments.ttl_seconds)


class CacheGetStr(UDFBase[CacheGetStrArguments, str]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: CacheGetStrArguments) -> str:
        if all(arguments.when_all) is not True:
            return arguments.default
        return cache_client.get_str(arguments.key, arguments.default)


class CacheGetInt(UDFBase[CacheGetIntArguments, int]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: CacheGetIntArguments) -> int:
        if all(arguments.when_all) is not True:
            return arguments.default
        return cache_client.get_int(arguments.key, arguments.default)


class CacheGetFloat(UDFBase[CacheGetFloatArguments, float]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: CacheGetFloatArguments) -> float:
        if all(arguments.when_all) is not True:
            return arguments.default
        return cache_client.get_float(arguments.key, arguments.default)


class IncrementWindow(UDFBase[IncrementWindowArguments, int]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: IncrementWindowArguments) -> int:
        if all(arguments.when_all) is False:
            return 0
        return cache_client.increment_window(arguments.key, arguments.window_seconds, arguments.max_ttl_seconds)


class GetWindowCount(UDFBase[CacheWindowArgumentsBase, int]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: CacheWindowArgumentsBase) -> int:
        if all(arguments.when_all) is False:
            return 0
        return cache_client.get_window_count(arguments.key, arguments.window_seconds)
