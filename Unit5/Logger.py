import conn_redis
import redis
import logging
import time
import datetime
import Unit5.ServiceDiscoveryAndConfiguration as configuration

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


@configuration.redis_connection('logs')
def log_recent(fun_conn, name, message, severity=logging.INFO, pipe=None):
    """ redis 记录不同日志和等级的最近100条日志

    @param fun_conn:
    @param string name: 日志名称
    @param string message: 日志内容
    @param string|int severity: 日志等级
    @param pipe:
    @return:

    """

    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = "recent:%s:%s" % (name, severity)
    # 为日志记录时间
    message = time.asctime() + ' ' + message
    pipe = pipe or fun_conn.pipeline()
    pipe.lpush(destination, message)
    # 对日志列表进行修剪，只包含最新的100条消息
    pipe.ltrim(destination, 0, 99)
    pipe.execute()


def log_common(name, message, severity=logging.INFO, timeout=5):
    """ 按小时记录日志出现的次数

    @param string name: 日志名称
    @param string message: 日志内容
    @param string|int severity: 日志等级
    @param int timeout: 超时时间
    @return:
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
            # 判断有没有设定开始计数的时间，如果有并且，把历史的日志存为历史。
            existing = pipe.get(start_key)
            pipe.multi()
            if existing and existing < hour_start:
                pipe.rename(start_key, destination + ':pstart')
                pipe.rename(destination, destination + ':last')
                pipe.set(start_key, hour_start)
            pipe.zincrby(destination, 1, message)
            log_recent(conn, name, message, severity, pipe)
            return
        except redis.exceptions.WatchError:
            continue


if __name__ == "__main__":
    # log_recent('sb', 'sunchen is sb', logging.WARN)
    log_common('sb', 'sunchen is 2sb', logging.WARN)
    pass
