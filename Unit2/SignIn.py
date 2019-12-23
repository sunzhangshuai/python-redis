import time
import conn_redis

conn = conn_redis.conn
QUIT = False
LIMIT = 10000000


def check_token(token):
    """ 验证登录

    @param string token: 登录标识
    @return:
    """
    return conn.hget('login:', token)


def update_token(token, user, item=None):
    """ 更新登录时间

    @param string token: 登录标识
    @param string user: 用户
    @param string item: 浏览内容
    @return:
    """

    login_time = time.time()
    pipe = conn.pipeline(False)
    pipe.hset('login:', token, user)
    pipe.zadd('recent:', {token: login_time})

    if item:
        pipe.zadd('viewed:' + token, {item: login_time})
        pipe.zremrangebyrank('viewed:' + token, 0, -26)
        pipe.zincrby('viewed:', -1, item)

    pipe.execute()


def update_token_pipeline(token, user, item=None):
    """ 更新登录时间

    @param string token: 登录标识
    @param string user: 用户
    @param string item: 浏览内容
    @return:
    """

    login_time = time.time()
    pipe = conn.pipeline(False)
    pipe.hset('login:', token, user)
    pipe.zadd('recent:', {token: login_time})

    if item:
        pipe.zadd('viewed:' + token, {item: login_time})
        pipe.zremrangebyrank('viewed:' + token, 0, -26)
        pipe.zincrby('viewed:', -1, item)

    pipe.execute()


def clean_sessions():
    """ 定期清理旧的回话数据

    """
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue
        # 最多清除1--个
        end_index = min(size - LIMIT, 100)
        tokens = conn.zrange('recent:', 0, end_index - 1)
        session_keys = []
        # 删除用户浏览记录
        for token in tokens:
            session_keys.append('viewed:' + token)
            session_keys.append('card:' + token)
        conn.delete(*session_keys)
        # 删除用户
        conn.hdel('login:', *tokens)
        conn.zrem('recent:', *tokens)
