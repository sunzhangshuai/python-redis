import conn_redis
import redis
import logging
import time
import datetime

conn = conn_redis.conn

# 日志登记
SEVERITY = {
    logging.DEBUG: "debug",
    logging.INFO: "info",
    logging.WARN: "warn",
    logging.ERROR: "error",
    logging.CRITICAL: "critical"
}

SEVERITY.update((name, name) for name in list(SEVERITY.values()))


def log_recent(name, message, severity=logging.INFO, pipe=None):
    """ redis 记录不同日志和等级的最近100条日志

    :param string name: 日志名称
    :param string message: 日志内容
    :param string severity: 日志等级
    :param pipe:
    :return:
    """

    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = "recent:%s:%s" % (name, severity)
    # 为日志记录时间
    message = time.asctime() + ' ' + message
    pipe = pipe or conn.pipeline()
    pipe.lpush(destination, message)
    # 对日志列表进行修剪，只包含最新的100条消息
    pipe.ltrim(destination, 0, 99)
    pipe.execute()


def log_common(name, message, severity=logging.INFO, timeout=5):
    """ 按小时记录日志出现的次数

    :param name:
    :param message:
    :param severity:
    :param timeout:
    :return:
    """
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = "common:%s:%s" % (name, severity)
    start_key = destination + ":start"
    pipe = conn.pipeline()
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.datetime.utcnow().timetuple()
            hour_start = datetime.datetime(*now[:4]).isoformat()
            # 判断有没有设定开始计数的时间，如果没有，则不处理
            existing = pipe.get(start_key)
            pipe.multi()
            if existing and existing < hour_start:
                pipe.rename(start_key, destination + ':pstart')
                pipe.rename(destination, destination + ':last')
                pipe.set(start_key, hour_start)
            pipe.zincrby(destination, 1, message)
            log_recent(name, message, severity, pipe)
            return
        except redis.exceptions.WatchError:
            continue


if __name__ == "__main__":
    # log_recent('sb', 'sunchen is sb', logging.WARN)
    log_common("sb", "sunchen is 2sb", logging.WARN)