import redis
import time

mpool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
mconn = redis.Redis(connection_pool=mpool)

spool = redis.ConnectionPool(host='localhost', port=6380, decode_responses=True)
sconn = redis.Redis(connection_pool=spool)


def wait_for_sync():
    """ 等待同步

    @return:
    """
    mconn.mset({
        "sunchen": "meinv",
        "sunchen1": "meinv",
        "sunchen2": "meinv",
        "sunchen3": "meinv",
        "sunchen4": "meinv"
    })
    while not sconn.info()['master_link_status'] == 'up':
        time.sleep(.001)
    while not sconn.get('sunchen'):
        time.sleep(.001)
    deadline = time.time() + 1.01
    while time.time() < deadline:
        if sconn.info()['aof_pending_bio_fsync'] == 0:
            break
        time.sleep(.001)
    mconn.delete("sunchen", "sunchen1", "sunchen2", "sunchen3", "sunchen4")


def list_item(itemid, sellerid, price):
    """ 上架商品

    @param itemid: 商品id
    @param sellerid: 卖家id
    @param price: 商品价格
    @return: 上架是否成功
    """
    inventory = "inventory:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    end = time.time() + 5
    pipeline = mconn.pipeline()

    while time.time() < end:
        try:
            pipeline.watch(inventory)
            if not pipeline.sismember(inventory, itemid):
                pipeline.unwatch(inventory)
                return None
            pipeline.multi()
            pipeline.zadd('market:', {item: price})
            pipeline.srem(inventory, itemid)
            pipeline.execute()
            return True
        except redis.exceptions.WatchError:
            pass
    return False


def purchase_item(buyerid, sellerid, itemid, lprice):
    """ 出售商品

    @param buyerid:
    @param sellerid:
    @param itemid:
    @param lprice:
    @return:
    """

    inventory = "inventory:%s" % buyerid
    buyer = "user:%s" % buyerid
    seller = "user:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)

    end = time.time() + 5
    pipeline = mconn.pipeline()

    while time.time() < end:
        try:
            pipeline.watch('market:', buyer)
            price = pipeline.zscore('market:', item)
            funds = int(pipeline.hget(buyer, 'funds'))
            if price != lprice or price > funds:
                pipeline.unwatch('market:', buyer)
                return None
            pipeline.multi()
            pipeline.zrem('market:', item)
            pipeline.sadd(inventory, itemid)
            pipeline.hincrby(buyer, 'funds', int(-lprice))
            pipeline.hincrby(seller, 'funds', int(lprice))
            pipeline.execute()
            return True
        except redis.exceptions.WatchError:
            pass
    return False


def update_token(token, user, item=None):
    """ 更新登录时间

    @param token:
    @param user:
    @param item:
    @return:
    """

    login_time = time.time()
    mconn.hset('login:', token, user)
    mconn.zadd('recent:', {token: login_time})

    if item:
        mconn.zadd('viewed:' + token, {item: login_time})
        mconn.zremrangebyrank('viewed:' + token, 0, -26)
        mconn.zincrby('viewed:', -1, item)


def update_token_pipeline(token, user, item=None):
    """ 更新登录时间

    @param token:
    @param user:
    @param item:
    @return:
    """

    login_time = time.time()
    pipe = mconn.pipeline(False)
    pipe.hset('login:', token, user)
    pipe.zadd('recent:', {token: login_time})

    if item:
        pipe.zadd('viewed:' + token, {item: login_time})
        pipe.zremrangebyrank('viewed:' + token, 0, -26)
        pipe.zincrby('viewed:', -1, item)

    pipe.execute()


def benchmark_update_token(duration):
    """ 效率测试

    @param duration:
    @return:
    """

    for function in (update_token, update_token_pipeline):
        count = 0
        start = time.time()
        end = start + duration

        while time.time() < end:
            count += 1
            function('token', 'user', 'item')
        delta = time.time() - start
        print(function.__name__, count, delta, count / delta)


if __name__ == "__main__":
    # pipe = mconn.pipeline(False)
    # pipe.hmset('user:1', {'name': 'zhang', 'funds': 200})
    # pipe.hmset('user:2', {'name': 'sun', 'funds': 200})
    # pipe.hmset('user:3', {'name': 'meng', 'funds': 200})
    # pipe.hmset('user:4', {'name': 'zhao', 'funds': 200})
    # pipe.sadd('inventory:1', 'ItemA', 'ItemB', 'ItemC')
    # pipe.sadd('inventory:2', 'ItemD', 'ItemE', 'ItemF')
    # pipe.sadd('inventory:3', 'ItemG', 'ItemH', 'ItemI')
    # pipe.sadd('inventory:4', 'ItemJ', 'ItemK', 'ItemL')
    # pipe.execute()
    # list_item('ItemD', 2, 50)
    # list_item('ItemE', 2, 40)
    # list_item('ItemJ', 4, 30)
    # list_item('ItemK', 4, 20)
    # list_item('ItemB', 1, 10)
    #
    # purchase_item(3, 2, 'ItemD', 50)

    benchmark_update_token(10)
