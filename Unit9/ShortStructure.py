import conn_redis
import time

conn = conn_redis.conn


def long_ziplist_performance(key, length, passes, psize):
    conn.delete(key)
    conn.sadd(key, *range(length))
    pipe = conn.pipeline(False)
    t = time.time()
    for p in range(passes):
        for pi in range(psize):
            pipe.smove(key, key, 1)
        pipe.execute()
    print(conn.debug_object('suntest'))
    return (passes * psize) / ((time.time() - t) or .001)


if __name__ == '__main__':
    times = long_ziplist_performance('suntest', 1, 1000, 100)
    print('长度为1,redis每秒可执行' + str(times) + '次')
    times = long_ziplist_performance('suntest', 100, 1000, 100)
    print('长度为100,redis每秒可执行' + str(times) + '次')
    times = long_ziplist_performance('suntest', 1000, 1000, 100)
    print('长度为1000,redis每秒可执行' + str(times) + '次')
    times = long_ziplist_performance('suntest', 5000, 1000, 100)
    print('长度为5000,redis每秒可执行' + str(times) + '次')
    times = long_ziplist_performance('suntest', 10000, 1000, 100)
    print('长度为10000,redis每秒可执行' + str(times) + '次')
    times = long_ziplist_performance('suntest', 50000, 1000, 100)
    print('长度为50000,redis每秒可执行' + str(times) + '次')
    times = long_ziplist_performance('suntest', 100000, 1000, 100)
    print('长度为100000,redis每秒可执行' + str(times) + '次')
    # conn.zadd('laosun1', {
    #     'a': 1,
    #     'b': 2,
    #     # 65 * 'a': 3
    # })
    # debug = conn.debug_object('laosun1')
    #
    # conn.lpush('laopo', 5, 2, 0, 1, 3, 1, 4)
    # debug_sun = conn.debug_object('laopo')
    # print(debug)
    # print(debug_sun)

    # conn.sadd('test', *range(500))
    # debug_sun = conn.debug_object('test')
    # print(debug_sun)
    # conn.sadd('test', *range(500, 1000))
    # debug_sun = conn.debug_object('test')
    # print(debug_sun)
