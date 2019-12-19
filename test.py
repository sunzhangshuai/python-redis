import redis
import time
import json

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)


def send_sold_email_via_queue(seller, item, price, buyer):
    """ 待发邮件入队

    @param seller:
    @param item:
    @param price:
    @param buyer:
    @return:
    """

    data = {
        "seller_id": seller,
        "item_id": item,
        "price": price,
        "buyer_id": buyer,
        "time": time.time()
    }
    conn.rpush("queue:mail", json.dumps(data))


def process_sold_email_queue():
    """ 执行发邮件队列中的任务

    @return:
    """

    while True:
        packed = conn.blpop(["queue:email"], 30)
        if not packed:
            continue
        to_send = json.loads(packed[1])
        try:
            fetch_data_and_send_sold_email(to_send)
        except RuntimeError as err:
            pass
        else:
           pass


def fetch_data_and_send_sold_email(to_send):
    return to_send


if __name__ == "__main__":
    print(111)
    # send_sold_email_via_queue(3, 6, 20.0, 4)
    # process_sold_email_queue()
