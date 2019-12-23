import conn_redis
import time
import json

conn = conn_redis.conn
QUIT = False


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
