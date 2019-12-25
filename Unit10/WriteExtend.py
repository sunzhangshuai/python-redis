import redis
import functools
from Unit5 import ServiceDiscoveryAndConfiguration as configuration
from Unit9 import FragmentStructure
import datetime
import math


def get_redis_connection(component, wait=1):
    """
    根据指定名字的配置获取Redis连接的函数
    @param str component: 指定名称
    @param int wait: 等待时间
    @return:
    """
    redis_key = 'config:redis:' + component
    old_config = configuration.CONFIGS.get(redis_key)
    now_config = configuration.get_config('redis:', component, wait)
    if now_config != old_config:
        configuration.REDIS_CONNECTIONS[redis_key] = redis.Redis(**now_config)
    return configuration.REDIS_CONNECTIONS.get(redis_key)


def get_sharded_connection(component, key, shard_count, wait=1):
    """
    基于分片信息获取一个连接
    @param str component: 指定名称
    @param int|str key: 指定标识
    @param int shard_count: 预计的成员总数
    @param int wait: 等待时间
    @return: redis连接
    """
    shard = FragmentStructure.shard_key(component, 'x' + str(key), shard_count, 2)
    return get_redis_connection(shard, wait)


def sharded_connection(component, shard_count, wait=1):
    """
    一个支持分片功能的连接装饰器
    @param str component: 指定名称
    @param int shard_count: 预计的成员总数
    @param int wait: 等待时间
    @return:
    """
    def wrapper(function):
        @functools.wraps(function)
        def call(key, *args, **kwargs):
            conn = get_sharded_connection(component, key, shard_count, wait)
            return function(conn, key, *args, **kwargs)
        return call
    return wrapper


@sharded_connection('unique', 16)
def count_visit(conn, uid):
    """ 计算唯一访客量

    @param object conn: 连接
    @param str uid: 用户id
    @return:
    """
    today = datetime.date.today()
    key = "unique:%s" % today.isoformat()
    conn2, expected = FragmentStructure.get_expected(conn, key, today)
    member = int(uid.replace("-", "")[:15], 16)
    if FragmentStructure.shard_sadd(conn2, key, member, expected, FragmentStructure.SHARD_SIZE):
        conn2.incr(key)


@configuration.redis_connection('unique')
def get_expected(conn, key, today):
    """ 每天预计的访问量

        @param conn:
        @param key:
        @param today:
        @return:
        """
    if key in FragmentStructure.EXPECTED:
        return FragmentStructure.EXPECTED[key]
    ex_key = key + ":expected"
    expected = conn.get(ex_key)
    if not expected:
        yesterday = (today - datetime.timedelta(days=1)).isoformat()
        expected = conn.get("unique:%s" % yesterday)
        expected = int(expected or FragmentStructure.DAILY_EXPECTED)
        expected = 2 ** int(math.ceil(math.log(expected * 1.5, 2)))
        if not conn.setnx(ex_key, expected):
            expected = conn.get(ex_key)
    FragmentStructure.EXPECTED[key] = expected
    return conn, FragmentStructure.EXPECTED[key]
