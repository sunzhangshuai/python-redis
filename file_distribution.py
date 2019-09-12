import redis
import collections
import chat
import os
import time

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)

aggregates = collections.defaultdict(lambda: collections.defaultdict(int))


def daily_country_aggregate(line):
    """ 一个本地聚合计算回调函数，每天以国家维度对日志进行聚合

    :param str line:
    :return:
    """
    if line:
        line = line.split()
        ip = line[0]
        day = line[1]
        country = find_city_by_ip_local(ip)[2]
        aggregates[day][country] += 1
        return
    for day, aggregate in aggregates.items():
        conn.zadd("daily:country:" + day, aggregate)
        del aggregates[day]


def find_city_by_ip_local(ip):
    """ 通过ip从本地查找城市

    :param ip:
    :return:
    """

    return ip


def copy_logs_to_redis(path, channel, count=10, limit=2 * 30, quit_when_done=True):
    """ 复制日志到redis

    :param path:
    :param channel:
    :param count:
    :param limit:
    :param quit_when_done:
    :return:
    """

    byte_in_redis = 0
    waiting = collections.deque()
    chat.create_chat("source", map(str, range(count)), "", channel)
    count = str(count)
    # 遍历所有日志文件
    for logfile in sorted(os.listdir(path)):
        full_path = os.path.join(path, logfile)
        fsize = os.stat(full_path).st_size
        # 如果程序需要更多空间，那么清除已经处理完毕的文件
        if byte_in_redis + fsize > limit:
            cleaned = _clean(channel, waiting, count)
            if cleaned:
                byte_in_redis -= cleaned
            else:
                time.sleep(.25)
        # 将文件上传至redis
        with open(full_path, "rb") as inp:
            block = " "
            while block:
                block = inp.read(2 ** 17)
                conn.append(channel + logfile, block)
        # 提醒监听者，文件已准备就绪
        chat.send_message("source", logfile, channel)
        # 对本地记录的redis内存占用量相关信息进行更新
        byte_in_redis += fsize
        waiting.append((logfile, fsize))
    # 所有日志文件已经处理完毕，向监听者报告此事
    if quit_when_done:
        chat.send_message("source", ":done", channel)
    # 在工作完成之后，清理无用的日志文件
    while waiting:
        cleaned = _clean(channel, waiting, count)
        if cleaned:
            byte_in_redis -= cleaned
        else:
            time.sleep(.25)


def _clean(channel, waiting, count):
    """ 对redis清理的详细步骤

    :param channel:
    :param waiting:
    :param count:
    :return:
    """

    while not waiting:
        return 0
    w0 = waiting[0][0]
    if conn.get(channel + w0 + ":done") == count:
        conn.delete(channel + w0 + ":done", channel + w0)
        return waiting.popleft()[1]
    return 0
