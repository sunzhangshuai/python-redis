import conn_redis
import redis
import uuid
import time

conn = conn_redis.conn


def acquire_lock(lock_name, acquire_timeout=10):
    """ redis构建锁

    @param string lock_name: 锁名称
    @param float acquire_timeout: 尝试获取锁的超时时间
    @return: 获取锁是否成功
    """
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx("lock:" + lock_name, identifier):
            return identifier
        time.sleep(.001)
    return False


def purchase_item_with_lock(buyer_id, seller_id, item_id, l_price):
    """ 购买商品

    @param int buyer_id: 买家
    @param int seller_id: 卖家
    @param int item_id: 出售商品id
    @param float l_price: 商品价格
    @return:
    """
    inventory = "inventory:%s" % buyer_id
    buyer = "user:%s" % buyer_id
    seller = "user:%s" % seller_id
    item = "%s.%s" % (item_id, seller_id)
    lock_name = "market"
    locked = acquire_lock(lock_name, 10)
    if not locked:
        return False
    try:
        pipeline = conn.pipeline()
        price = pipeline.zscore('market:', item)
        funds = int(pipeline.hget(buyer, 'funds'))
        if price != l_price or price > funds:
            return False
        pipeline.zrem('market:', item)
        pipeline.sadd(inventory, item_id)
        pipeline.hincrby(buyer, 'funds', int(-l_price))
        pipeline.hincrby(seller, 'funds', int(l_price))
        pipeline.execute()
        return True
    finally:
        release_lock(lock_name, locked)


def release_lock(lock_name, identifier):
    """ 释放锁

    @param string lock_name: 锁名称
    @param string identifier: 锁的唯一标识
    @return: 是否释放成功
    """
    lock = "lock:" + lock_name
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


def acquire_lock_with_timeout(lock_name, acquire_timeout=10, lock_timeout=10):
    """ redis构建带有超时时间的锁

    @param string lock_name: 锁名称
    @param float acquire_timeout: 尝试获取锁的超时时间
    @param float lock_timeout: 锁超时时间
    @return: 获取锁是否成功
    """

    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    lock_name = "lock:" + lock_name
    while time.time() < end:
        if conn.setnx(lock_name, identifier):
            conn.expire(lock_name, lock_timeout)
            return identifier
        elif not conn.ttl(lock_name):
            conn.expire(lock_name, lock_timeout)
        time.sleep(.001)
    return False


def acquire_semaphore(se_name, limit, timeout=10):
    """ 构建带有超时特性限制的计数信号量锁

    @param string se_name: 锁名称
    @param int limit: 允许同时获取锁的进程数
    @param int timeout: 超时时间
    @return: 锁标识
    """
    identifier = str(uuid.uuid4())
    now = time.time()
    pipe = conn.pipeline(True)
    pipe.zremrangebyscore(se_name, '-inf', now - timeout)
    pipe.zadd(se_name, {identifier: now})
    pipe.zrank(se_name, identifier)
    if pipe.execute()[-1] < limit:
        return identifier
    conn.zrem(se_name, identifier)
    return None


def release_semaphore(se_name, identifier):
    """ 释放计数信号量

    @param string se_name: 锁名称
    @param string identifier: 锁标识
    @return:
    """
    return conn.zrem(se_name, identifier)


def acquire_fair_semaphore(se_name, limit, timeout=10):
    """ 构建公平的信号量
    在分布式的服务器中，系统时间可能有快有慢

    @param string se_name: 锁名称
    @param int limit: 允许同时获取锁的进程数
    @param float timeout: 超时时间
    @return: 锁标识
    """
    identifier = str(uuid.uuid4())
    c_zset = se_name + ":owner"
    ctr = se_name + ":counter"
    now = time.time()
    pipe = conn.pipeline(True)
    pipe.zremrangebyscore(se_name, '-inf', now - timeout)
    pipe.zinterstore(c_zset, {c_zset: 1, se_name: 0})
    pipe.incr(ctr)
    count = pipe.execute()[-1]
    pipe.zadd(c_zset, {identifier: count})
    pipe.zadd(se_name, {identifier: now})
    pipe.zrank(c_zset, identifier)
    if pipe.execute()[-1] < limit:
        return identifier
    pipe.zrem(c_zset, identifier)
    pipe.zrem(se_name, identifier)
    pipe.execute()
    return None


def release_fair_semaphore(se_name, identifier):
    """ 释放公平的计数信号量

    @param string se_name: 锁名称
    @param string identifier: 锁标识
    @return:
    """

    pipe = conn.pipeline()
    pipe.zrem(se_name, identifier)
    pipe.zrem(se_name + ":owner", identifier)
    return pipe.execute()[0]


def refresh_fair_semaphore(se_name, identifier):
    """ 刷新信号量

    @param string se_name: 锁名称
    @param string identifier: 锁标识
    @return:
    """
    if conn.zadd(se_name, {identifier: time.time()}):
        release_fair_semaphore(se_name, identifier)
        return False
    return True


def acquire_semaphore_with_lock(se_name, limit, timeout=10):
    """ 消除公平信号量的竞争条件

    @param string se_name: 锁名称
    @param int limit: 允许同时获取锁的进程数
    @param int timeout: 超时时间
    @return:
    """

    identifier = acquire_lock(se_name, .001)
    if identifier:
        try:
            return acquire_fair_semaphore(se_name, limit, timeout)
        finally:
            release_lock(se_name, identifier)


if __name__ == '__main__':
    print(acquire_fair_semaphore('wodege', 4, 1))
    # print(acquire_lock('wodege'))
