import conn_redis

conn = conn_redis.conn


def add_to_card(token, item, count):
    """ 添加商品至购物车

    @param string token: 登录标识
    @param string item: 商品
    @param int count: 数量
    @return:
    """
    if count <= 0:
        conn.hdel('card:' + token, item)
    else:
        conn.hset('card:', item, count)
