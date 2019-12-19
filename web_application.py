import time
import redis
import json

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)

QUIT = False
LIMIT = 10000000


class Inventory(object):

    @staticmethod
    def get(row_id):
        return {'name': row_id}


def hash_request(request):
    return 'hhh' + request


def extract_item_id(request):
    return bool(request)


def is_dynamic(request):
    return bool(request)


def can_cache(request):
    # 尝试获取商品id
    item_id = extract_item_id(request)
    # 判断能否获取到，和网站是否动态
    if not item_id or is_dynamic(request):
        return False

    rank = conn.zrank('viewed:', item_id)
    return rank is not None and rank < 10000


def check_token(token):
    """ 验证登录

    @param token:
    @return:
    """
    return conn.hget('login:', token)


# def update_token(token, user, item=None):
#     """ 更新登录时间
#
#     @param token:
#     @param user:
#     @param item:
#     @return:
#     """
#
#     login_time = time.time()
#     conn.hset('login:', token, user)
#     conn.zadd('recent:', {token: login_time})
#
#     if item:
#         conn.zadd('viewed:' + token, item, time)
#         conn.zremrangebyrank('viewed:' + token, 0, -26)


def update_token(token, user, item=None):
    """ 更新登录时间

    @param token:
    @param user:
    @param item:
    @return:
    """

    login_time = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', {token: login_time})

    if item:
        conn.zadd('viewed:' + token, {item: login_time})
        conn.zremrangebyrank('viewed:' + token, 0, -26)
        conn.zincrby('viewed:', -1, item)


def rescale_viewed():
    """ 守护进程，定期清理不常被浏览的商品缓存

    @return:
    """

    if not QUIT:
        conn.zremrangebyrank('viewed:', 20000, -1)
        conn.zinterstore('viewed:', {'viewed:': 0.5})
        time.sleep(300)


def clean_sessions():
    """ 定期清理旧的回话数据

    @return:
    """
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue

        end_index = min(size - LIMIT, 100)

        tokens = conn.zrange('recent:', 0, end_index - 1)

        session_keys = []

        # 删除用户浏览记录
        for token in tokens:
            session_keys.append('viewed:' + token)
            session_keys.append('card:' + token)

        conn.delete(*session_keys)

        # 删除用户
        conn.hdel('login:', *tokens)
        conn.zrem('recent:', *tokens)


def add_to_card(token, item, count):
    """ 购物车

    @param token:
    @param item:
    @param count:
    @return:
    """

    if count <= 0:
        conn.hdel('card:' + token, item)
    else:
        conn.hset('card:', item, count)


def cache_request(request, callback):
    """ 通过缓存请求页面

    @param request:
    @param callback:
    @return:
    """

    if not can_cache(request):
        return callback(request)

    page_key = 'cache:' + hash_request(request)
    content = conn.get(page_key)
    if not content:
        content = callback(request)
        conn.setex(request, 300, content)

    return content


def schedule_row_cache(row_id, delay):
    """ 添加促销调度

    @param row_id:
    @param delay:
    @return:
    """
    conn.zadd('delay:', {row_id, delay})
    conn.zadd('schedule:', {row_id, time.time()})


def cache_rows():
    """ 守护进程 缓存数据

    @return:
    """
    while not QUIT:
        job = conn.zrange('schedule:', 0, 0, withscores=True)

        now = time.time()
        if not next or job[0][1] > now:
            time.sleep(.05)
            continue

        row_id = job[0][0]
        delay = conn.zscore('delay:', row_id)
        if delay <= 0:
            # 删除调度
            conn.zrem('delay:', row_id)
            conn.zrem('schedule:', row_id)
            conn.delete('inv:' + row_id)
            continue

        row = Inventory.get(row_id)

        conn.zadd('schedule:', {row_id, now + delay})
        conn.set('inv:' + row_id, json.dumps(row))


if __name__ == '__main__':
    # update_token('token', 'sunsun', item='apple')
    rescale_viewed()
