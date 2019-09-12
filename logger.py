import redis
import logging
import time
import datetime

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)

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

    :param name:
    :param message:
    :param severity:
    :param pipe:
    :return:
    """

    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = "recent:%s:%s" % (name, severity)
    message = time.asctime() + ' ' + message
    pipe = pipe or conn.pipeline()
    pipe.lpush(destination, message)
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
    log_common("sb", "sunchen is sb", logging.WARN)
