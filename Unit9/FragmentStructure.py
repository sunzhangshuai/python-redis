# 分片结构
import conn_redis
import binascii
import datetime
import math
import uuid

SHARD_SIZE = 512
DAILY_EXPECTED = 1000000
EXPECTED = {}


def shard_key(base, key, total_elements, shard_size):
    """
    获取分片后的key
    @param base: 基础key
    @param key: 要存的map key
    @param total_elements: 预计的成员总数
    @param shard_size: 每个分片的尺寸
    @return:
    """

    if isinstance(key, int) or key.isdigit():
        shard_id = int(str(key), 10) // shard_size
    else:
        shards = 2 * total_elements // shard_size
        shard_id = binascii.crc32(key.encode()) % shards
    return "%s%s" % (base, shard_id)


def shard_hset(conn, base, key, value, total_elements, shard_size):
    """ 分片版 hset

    @param conn:
    @param base:
    @param key:
    @param value:
    @param total_elements:
    @param shard_size:
    @return:
    """

    shard = shard_key(base, key, total_elements, shard_size)
    return conn.hset(shard, key, value)


def shard_hget(conn, base, key, total_elements, shard_size):
    """ 分片版 hget

    @param conn:
    @param base:
    @param key:
    @param total_elements:
    @param shard_size:
    @return:
    """

    shard = shard_key(base, key, total_elements, shard_size)
    return conn.hget(shard, key)


def shard_sadd(conn, base, member, total_elements, shard_size):
    """ 分片sadd

    @param conn:
    @param base:
    @param member:
    @param total_elements:
    @param shard_size:
    @return:
    """
    shard_id = shard_key(base, "X" + str(member), total_elements, shard_size)
    return conn.sadd(shard_id, member)


def count_visit(conn, uid):
    """ 计算唯一访客量

    @param conn:
    @param str uid:
    @return:
    """
    today = datetime.date.today()
    key = "unique:%s" % today.isoformat()
    expected = get_expected(conn, key, today)
    member = int(uid.replace("-", "")[:15], 16)
    if shard_sadd(conn, key, member, expected, SHARD_SIZE):
        conn.incr(key)


def get_expected(conn, key, today):
    """ 每天预计的访问量

    @param conn:
    @param key:
    @param today:
    @return:
    """
    if key in EXPECTED:
        return EXPECTED[key]
    ex_key = key + ":expected"
    expected = conn.get(ex_key)
    if not expected:
        yesterday = (today - datetime.timedelta(days=1)).isoformat()
        expected = conn.get("unique:%s" % yesterday)
        expected = int(expected or DAILY_EXPECTED)
        expected = 2 ** int(math.ceil(math.log(expected * 1.5, 2)))
        if not conn.setnx(ex_key, expected):
            expected = conn.get(ex_key)
    EXPECTED[key] = expected
    return EXPECTED[key]


if __name__ == '__main__':
    for i in range(100000):
        session_id = uuid.uuid4()
        conn1 = conn_redis.conn
        count_visit(conn1, str(session_id))
