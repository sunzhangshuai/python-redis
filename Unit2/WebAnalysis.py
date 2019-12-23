import time
import conn_redis

conn = conn_redis.conn
QUIT = False


def rescale_viewed():
    """
    守护进程，定期清理不常被浏览的商品缓存
    """

    if not QUIT:
        conn.zremrangebyrank('viewed:', 20000, -1)
        conn.zinterstore('viewed:', {'viewed:': 0.5})
        time.sleep(300)
