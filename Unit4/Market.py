import conn_redis
import time
import redis

conn = conn_redis.conn


def list_item(item_id, seller_id, price):
    """ 上架商品

    @param string item_id: 商品id
    @param string seller_id: 卖家id
    @param float price: 商品价格
    @return: 上架是否成功
    """
    inventory = "inventory:%s" % seller_id
    item = "%s.%s" % (item_id, seller_id)
    end = time.time() + 5
    pipeline = conn.pipeline()

    while time.time() < end:
        try:
            pipeline.watch(inventory)
            if not pipeline.sismember(inventory, item_id):
                pipeline.unwatch()
                return None
            pipeline.multi()
            pipeline.zadd('market:', {item: price})
            pipeline.srem(inventory, item_id)
            pipeline.execute()
            return True
        except redis.exceptions.WatchError:
            pass
    return False


def purchase_item(buyer_id, seller_id, item_id, l_price):
    """ 出售商品

    @param string buyer_id: 买家
    @param string seller_id: 卖家
    @param string item_id: 商品id
    @param float l_price: 价格
    @return:
    """

    inventory = "inventory:%s" % buyer_id
    buyer = "user:%s" % buyer_id
    seller = "user:%s" % seller_id
    item = "%s.%s" % (item_id, seller_id)

    end = time.time() + 5
    pipeline = conn.pipeline()

    while time.time() < end:
        try:
            pipeline.watch('market:', buyer)
            price = pipeline.zscore('market:', item)
            funds = int(pipeline.hget(buyer, 'funds'))
            if price != l_price or price > funds:
                pipeline.unwatch()
                return None
            pipeline.multi()
            pipeline.zrem('market:', item)
            pipeline.sadd(inventory, item_id)
            pipeline.hincrby(buyer, 'funds', int(-l_price))
            pipeline.hincrby(seller, 'funds', int(l_price))
            pipeline.execute()
            return True
        except redis.exceptions.WatchError:
            pass
    return False


if __name__ == "__main__":
    # pipe = conn.pipeline(False)
    # pipe.hmset('user:zhang', {'name': 'zhang', 'funds': 200})
    # pipe.hmset('user:sun', {'name': 'sun', 'funds': 200})
    # pipe.hmset('user:meng', {'name': 'meng', 'funds': 200})
    # pipe.hmset('user:zhao', {'name': 'zhao', 'funds': 200})
    # pipe.sadd('inventory:zhang', 'ItemA', 'ItemB', 'ItemC')
    # pipe.sadd('inventory:sun', 'ItemD', 'ItemE', 'ItemF')
    # pipe.sadd('inventory:meng', 'ItemG', 'ItemH', 'ItemI')
    # pipe.sadd('inventory:zhao', 'ItemJ', 'ItemK', 'ItemL')
    # pipe.execute()
    list_item('ItemD', 'meng', 50)
    list_item('ItemE', 'sun', 40)
    list_item('ItemJ', 'zhao', 30)
    list_item('ItemK', 'zhao', 20)
    list_item('ItemB', 'zhang', 10)
    print(purchase_item('meng', 'sun', 'ItemD', 50))
