import contextlib

import redis
import conn_redis
import time
import datetime
import bisect
import uuid

conn = conn_redis.conn

QUIT = False
SAMPLE_COUNT = 120

PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]


def update_counter(name, count=1, now=None):
    """ 更新计数器

    @param string name: 计数器名称
    @param int count: 计数器数量
    @param int now: 当前时间
    @return:
    """

    now = now or time.time()
    pipe = conn.pipeline()
    for prec in PRECISION:
        pnow = int(now / prec) * prec
        str_hash = "%s:%s" % (prec, name)
        pipe.zadd('known:', {str_hash: 0})
        pipe.hincrby('count:' + str_hash, pnow, count)
    pipe.execute()


def get_counter(name, prec, reverse=False):
    """ 获取指定计数器的计数数据

    @param string name: 计数器名称
    @param int prec: 计数维度
    @param bool reverse: 是否倒序
    @return:
    """

    str_hash = "%s:%s" % (prec, name)
    data = conn.hgetall('count:' + str_hash)
    to_return = []
    for (key, value) in data.items():
        to_return.append((int(key), int(value)))
    to_return.sort(reverse=reverse)
    return to_return


def clean_counter():
    """ 定期清理计数器

    @return:
    """

    pipe = conn.pipeline()
    passes = 0
    while not QUIT:
        index = 0
        start = time.time()
        while index < conn.zcard("known:"):
            str_hash = conn.zrange("known:", index, index)
            # 计算下一次要清理的计数器的索引
            index += 1
            # 清理到最后一个，等待进入下一次循环
            if not str_hash:
                break
            # 获取计数维度
            str_hash = str_hash[0]
            prec = int(str_hash.split(":")[0])
            # 超过1分钟的计数器清理时间于计数维度保持一致，小于的每分钟清理一次
            bprec = (prec // 60) or 1
            if passes % bprec:
                continue
            # 获取超过两小时的计数器数据
            hkey = "count:" + str_hash
            simples = list(map(int, conn.hkeys(hkey)))
            simples.sort()
            cutoff = time.time() - SAMPLE_COUNT * prec
            remove = bisect.bisect_right(simples, cutoff)
            if remove:
                conn.hdel(hkey, *simples[:remove])
                if remove == len(simples):
                    try:
                        pipe.watch(str_hash)
                        pipe.multi()
                        # 如果计数器中的元素都需要删除，则删除整个计数器
                        if not pipe.hlen(hkey):
                            pipe.zrem("known:", hash)
                            pipe.delete(hkey)
                            pipe.execute()
                            index -= 1
                        else:
                            pipe.unwatch()
                    except redis.exceptions.WatchError:
                        pass
                    except redis.exceptions.RedisError:
                        pass
        passes += 1
        duration = min(int(time.time() - start) + 1, 60)
        time.sleep(max(60 - duration, 1))


def update_stats(context, value, str_type, timeout=5):
    """ 存储统计数据
    最终统计数据结构如下：
    {
        "min": 1,
        "max": 5,
        "sum": 200,
        "sumsq": 3000,
        "count": 150
    }

    @param string context: 上文
    @param int value: 所用时间
    @param string str_type: 下文
    @param int timeout: 程序超时时间
    @return:
    """

    destination = "stats:%s:%s" % (context, str_type)
    start_key = destination + ":start"
    end = time.time() + timeout
    pipe = conn.pipeline()
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.datetime.utcnow().timetuple()
            hour_start = datetime.datetime(*now[:4]).isoformat()
            existing = pipe.get(start_key)
            pipe.multi()
            # 判断计数开始时间
            if existing and existing < hour_start:
                pipe.rename(start_key, destination + ":pstart")
                pipe.rename(destination, destination + ":last")
                pipe.set(start_key, hour_start)
            # 更新统计数据
            tkey1 = str(uuid.uuid4())
            tkey2 = str(uuid.uuid4())
            pipe.zadd(tkey1, {"min": value})
            pipe.zadd(tkey2, {"max": value})
            pipe.zunionstore(destination, [tkey1, destination], aggregate="min")
            pipe.zunionstore(destination, [tkey2, destination], aggregate="max")
            pipe.delete(tkey1, tkey2)
            pipe.zincrby(destination, 1, "count")
            pipe.zincrby(destination, value, "sum")
            pipe.zincrby(destination, value ** 2, "sumsq")
            return pipe.execute()[-3:]
        except redis.exceptions.WatchError:
            continue


def get_stats(context, str_type):
    """ 获取统计数据

    @param string context: 上文
    @param string str_type: 下文
    @return dict:
    """

    destination = "stats:%s:%s" % (context, str_type)
    if not conn.exists(destination):
        return []
    data = dict(conn.zrange(destination, 0, -1, withscores=True))
    # 计算平均数
    data["average"] = data["sum"] / data["count"]
    # 计算方差
    data["stddev"] = (
        (
                data["sumsq"] / data["count"] +
                data["average"] ** 2 +
                2 * data["average"] * data["sum"]
        ) ** 0.5
    )
    return data


@contextlib.contextmanager
def access_time(context):
    """ 上下文管理器

    @param context:
    @return:
    """

    start = time.time()
    yield
    delta = int(time.time() - start)
    stats = update_stats(context, delta, 'AccessTime')
    average = stats[1] / stats[0]
    pipe = conn.pipeline()
    pipe.zadd("slowest:AccessTime", {context: average})
    pipe.zremrangebyrank("slowest:AccessTime", 0, -101)
    pipe.execute()


def process_view(callback, path, **arg):
    """ 上下文管理器使用方法

    @param callback:
    @param path:
    @return:
    """
    with access_time(path):
        return callback(**arg)


if __name__ == '__main__':
    # update_counter('xiaoxunxun_counter', 1)
    # print(get_counter('xiaoxunxun_counter', 5, True))
    # clean_counter()
    # clean_counter()
    # update_stats('context', 20, 'str_type')
    print(process_view(get_stats, 'get_stats', context='context', str_type='str_type'))
    # print(get_stats('context', 'str_type'))
    print(get_stats(**{'context': 'context', 'str_type': 'str_type'}))
    # update_counter('sun', 2)
    # update_counter('zhang', 3)
