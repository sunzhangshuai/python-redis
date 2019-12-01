import redis
import uuid
import time
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)


def acquire_lock(lockname, acquire_timeout=10):
    """ redis构建锁

    :param lockname: 锁名称
    :param float acquire_timeout: 尝试获取锁的超时时间
    :return: 获取锁是否成功
    """

    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx("lock:" + lockname, identifier):
            return identifier
        time.sleep(.001)
    return False


def purchase_item_with_lock(buyerid, sellerid, itemid, lprice):
    """ 购买商品

    :param buyerid: 买家
    :param sellerid: 卖家
    :param itemid: 出售商品id
    :param lprice: 商品价格
    :return:
    """

    inventory = "inventory:%s" % buyerid
    buyer = "user:%s" % buyerid
    seller = "user:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    lockname = "market"
    locked = acquire_lock(lockname, 10)
    if not locked:
        return False
    try:
        pipeline = conn.pipeline()
        price = pipeline.zscore('market:', item)
        funds = int(pipeline.hget(buyer, 'funds'))
        if price != lprice or price > funds:
            return False
        pipeline.zrem('market:', item)
        pipeline.sadd(inventory, itemid)
        pipeline.hincrby(buyer, 'funds', int(-lprice))
        pipeline.hincrby(seller, 'funds', int(lprice))
        pipeline.execute()
        return True
    finally:
        release_lock(lockname, locked)


def release_lock(lockname, identifier):
    """ 释放锁

    :param lockname: 锁名称
    :param identifier: 锁的唯一标识
    :return: 是否释放成功
    """
    lock = "lock:" + lockname
    pipe = conn.pipeline(True)
    while True:
        pipe.watch(lock)
        try:
            if pipe.get(lock) != identifier:
                pipe.unwatch()
                break
            pipe.multi()
            pipe.delete(lock)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass
    return False


def acquire_time_with_timeout(lockname, acquire_timeout=10, lock_timeout=10):
    """ redis构建带有超时时间的锁

    :param lockname: 锁名称
    :param acquire_timeout: 尝试获取锁的超时时间
    :param lock_timeout: 锁超时时间
    :return: 获取锁是否成功
    """

    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    lockname = "lock:" + lockname
    while time.time() < end:
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif not conn.ttl(lockname):
            conn.expire(lockname, lock_timeout)
        time.sleep(.001)
    return False


def acquire_semaphore(sename, limit, timeout=10):
    """ 构建带有超时特性限制的计数信号量锁

    :param sename: 锁名称
    :param limit: 允许同时获取锁的进程数
    :param timeout:
    :return: 信号量的过期时间
    """

    identifier = str(uuid.uuid4())
    now = time.time()
    pipe = conn.pipeline(True)
    pipe.zremrangebyscore(sename, '-inf', now-timeout)
    pipe.zadd(sename, {now: identifier})
    pipe.zrank(sename, identifier)
    if pipe.execute([-1]) < limit:
        return identifier
    conn.zrem(sename, identifier)
    return None


def release_semaphore(sename, identifier):
    """ 释放计数信号量

    :param sename:
    :param identifier:
    :return:
    """

    return conn.zrem(sename, identifier)


def acquire_fair_semaphore(sename, limit, timeout=10):
    """ 构建公平的信号量
    在分布式的服务器中，系统时间可能有快有慢

    :param sename:
    :param limit:
    :param timeout:
    :return:
    """

    identifier = str(uuid.uuid4())
    czset = sename + ":owner"
    ctr = sename + ":counter"
    now = time.time()
    pipe = conn.pipeline(True)
    pipe.zremrangebyscore(sename, "-inf", now - timeout)
    pipe.zinterstore(czset, {czset: 1, sename: 0})
    pipe.incr(ctr)
    count = pipe.execute()[-1]
    pipe.zadd(czset, {count: identifier})
    pipe.zadd(sename, {now: identifier})
    pipe.zrank(czset, identifier)
    if pipe.execute()[-1] < limit:
        return identifier
    pipe.zrem(czset, identifier)
    pipe.zrem(sename, identifier)
    pipe.execute()
    return None


def release_fair_semaphore(sename, identifier):
    """ 释放公平的计数信号量

    :param sename:
    :param identifier:
    :return:
    """

    pipe = conn.pipeline()
    pipe.zrem(sename, identifier)
    pipe.zrem(sename + ":owner", identifier)
    return pipe.execute()[0]


def refresh_fair_semaphore(sename, identifier):
    """ 刷新信号量

    :param sename:
    :param identifier:
    :return:
    """
    if conn.zadd(sename, {identifier: time.time()}):
        release_fair_semaphore(sename, identifier)
        return False
    return True


def acquire_semaphore_with_lock(sename, limit, timeout=10):
    """ 消除公平信号量的竞争条件

    :param sename:
    :param limit:
    :param timeout:
    :return:
    """

    identifier = acquire_lock(sename, .001)
    if identifier:
        try:
            return acquire_fair_semaphore(sename, limit, timeout)
        finally:
            release_lock(sename, identifier)
