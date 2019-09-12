import contextlib

import redis
import time
import datetime
import bisect
import uuid

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)
QUIT = False
SAMPLE_COUNT = 120

PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]


def update_counter(name, count=1, now=None):
    """ 更新计数器

    :param name:
    :param count:
    :param now:
    :return:
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
    """ 获取指定计数器的技术数据

    :param name:
    :param prec:
    :param reverse:
    :return:
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

    :return:
    """

    pipe = conn.pipeline()
    passes = 0
    while not QUIT:
        index = 0
        start = time.time()
        while index < conn.zcard("known:"):
            str_hash = conn.zrange("known:", index, index)
            index += 1
            if not str_hash:
                break
            str_hash = str_hash[0]
            prec = int(str_hash.split(":")[0])
            bprec = (prec // 60) or 1
            if passes % bprec:
                continue

            # 获取所有时间片
            hkey = "count:" + str_hash
            simples = list(map(int, conn.hkeys(hkey)))
            simples.sort()
            cutoff = time.time() - SAMPLE_COUNT * prec
            remove = bisect.bisect_right(simples, cutoff)
            if remove:
                conn.hdel(hkey, *simples[:remove])
                if remove == len(simples):
                    try:
                        pipe.watch(hash)
                        pipe.multi()
                        if not pipe.hlen(hkey):
                            pipe.zrem("known:", hash)
                            pipe.delete(hkey)
                            pipe.execute()
                            index -= 1
                        else:
                            pipe.unwatch()
                    except redis.exceptions.WatchError:
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

    :param context: 上文
    :param value: 所用时间
    :param str_type: 下文
    :param timeout: 程序超时时间
    :return:
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
            if existing and existing < hour_start:
                pipe.rename(start_key, destination + ":pstart")
                pipe.rename(destination, destination + ":last")
                pipe.set(start_key, hour_start)
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

    :param context:
    :param str_type:
    :return:
    """

    destination = "stats:%s:%s" % (context, str_type)
    data = conn.zrange(destination, 0, -1, withscores=True)
    data["average"] = data["sum"] / data["count"]
    data["stddev"] = (
        (
                data["sumsq"] / data["count"] +
                data["average"] ** 2 +
                2 * data["average"] * data["sum"]
        ) ** 0.5
    )


@contextlib.contextmanager
def access_time(context):
    """ 上下文管理器

    :param context:
    :return:
    """

    start = time.time()
    yield
    delta = time.time() - start
    stats = update_stats(context, delta, 'AccessTime')
    average = stats[1] / stats[0]
    pipe = conn.pipeline()
    pipe.zadd("slowest:AccessTime", {context: average})
    pipe.zremrangebyrank("slowest:AccessTime", 0, -101)
    pipe.execute()


def process_view(callback, path):
    """ 上下文管理器使用方法

    :param callback:
    :param path:
    :return:
    """
    with access_time(path):
        return callback()


if __name__ == '__main__':
    # update_counter('xiaoxunxun_counter', 1)
    # print(get_counter('xiaoxunxun_counter', 5, True))
    clean_counter()
