import redis
import time
import threading

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)


def notrans():
    print(conn.incr('notrans:'))
    time.sleep(0.1)
    print(conn.decr('notrans:'))


if 1:
    for i in [1, 2, 3]:
        threading.Thread(target=notrans).start()

    time.sleep(0.5)


def trans():
    pipeline = conn.pipeline()
    pipeline.incr('trans:')
    time.sleep(.5)
    pipeline.decr('trans:')
    result = pipeline.execute()
    print(result)


if 1:
    for i in [1, 2, 3]:
        threading.Thread(target=trans).start()

    time.sleep(0.5)
