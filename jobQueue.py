import redis
import time
import json
import uuid
import lock

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)


def send_sold_email_via_queue(seller, item, price, buyer):
    """ 待发邮件入队

    :param seller:
    :param item:
    :param price:
    :param buyer:
    :return:
    """

    new_data = {
        "seller_id": seller,
        "item_id": item,
        "price": price,
        "buyer_id": buyer,
        "time": time.time()
    }
    conn.rpush("queue:email", json.dumps(new_data))


def process_sold_email_queue():
    """ 执行发邮件队列中的任务

    :return:
    """

    while True:
        packed = conn.blpop(["queue:email"], 30)
        if not packed:
            continue
        to_send = json.loads(packed[1])
        try:
            fetch_data_and_send_sold_email(to_send)
        except RuntimeError:
            pass
        else:
            pass


def fetch_data_and_send_sold_email(to_send):
    return to_send


def worker_watch_queue(queue, callbacks):
    """ 执行多个任务

    :param queue:
    :param callbacks:
    :return:
    """

    while True:
        packed = conn.blpop([queue], 30)
        if not packed:
            continue
        name, args = json.loads(packed[1])
        if name not in callbacks:
            continue
        callbacks[name](*args)


def worker_watch_queues(queues, callbacks):
    """ 优先级队列

    :param queues:
    :param callbacks:
    :return:
    """

    while True:
        packed = conn.blpop(queues, 30)
        if not packed:
            continue
        name, args = json.loads(packed[1])
        if name not in callbacks:
            continue
        callbacks[name](*args)


def execute_later(queue, name, args, delay=0):
    """ 推入延时队列

    :param queue:
    :param name:
    :param args:
    :param delay:
    :return:
    """

    identifier = uuid.uuid4()
    item = json.dumps([identifier, queue, name, args])
    if delay > 0:
        conn.zadd("delayed:", {item: time.time() + delay})
    else:
        conn.rpush("queue:" + queue, item)


def poll_queue():
    """ 将延时队列内容移入任务队列

    :return:
    """

    while True:
        item = conn.zrange("delayed:", 0, 0, withscores=True)
        if item is None or item[0][1] > time.time():
            time.sleep(.001)
            continue
        identifier, queue, name, args = json.loads(item[0][0])
        locked = lock.acquire_lock(identifier, 10)
        if not locked:
            continue
        if conn.zrem("delayed:", item[0][0]):
            conn.rpush("queue:" + queue, item)
        lock.release_lock(identifier, locked)


if __name__ == "__main__":
    # send_sold_email_via_queue(3, 6, 20.0, 4)
    # process_sold_email_queue()
    data = ["send_email", [
        {
            "seller_id": 3,
            "item_id": 6,
            "price": 20.0,
            "buyer_id": 4,
            "time": time.time()
        },
        "sb"
    ]]
    conn.rpush("queue:email", json.dumps(data))
    worker_watch_queue("queue:email", {"send_email": fetch_data_and_send_sold_email})
    worker_watch_queues(["queue_email", "queue_send"], {})
