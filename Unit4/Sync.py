import redis
import time

m_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
m_conn = redis.Redis(connection_pool=m_pool)

s_pool = redis.ConnectionPool(host='localhost', port=6380, decode_responses=True)
s_conn = redis.Redis(connection_pool=s_pool)


def wait_for_sync():
    """ 等待同步

    @return:
    """
    m_conn.mset({
        "sunchen": "meinv",
        "sunchen1": "meinv",
        "sunchen2": "meinv",
        "sunchen3": "meinv",
        "sunchen4": "meinv"
    })
    while not s_conn.info()['master_link_status'] == 'up':
        time.sleep(.001)
    while not s_conn.get('sunchen'):
        time.sleep(.001)
    deadline = time.time() + 1.01
    while time.time() < deadline:
        if s_conn.info()['aof_pending_bio_fsync'] == 0:
            break
        time.sleep(.001)
    m_conn.delete("sunchen", "sunchen1", "sunchen2", "sunchen3", "sunchen4")
