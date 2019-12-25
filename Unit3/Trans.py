import conn_redis
import time
import threading

conn = conn_redis.conn


def no_trans():
    """
    非事务
    """
    print(conn.incr('no_trans:'))
    time.sleep(0.1)
    print(conn.decr('no_trans:'))


if True:
    for i in [1, 2, 3]:
        threading.Thread(target=no_trans).start()
    time.sleep(0.5)


def trans():
    """
    事务
    """
    pipeline = conn.pipeline()
    pipeline.incr('trans:')
    time.sleep(.5)
    pipeline.decr('trans:')
    result = pipeline.execute()
    print(result)


if True:
    for i in [1, 2, 3]:
        threading.Thread(target=trans).start()
    time.sleep(0.5)
