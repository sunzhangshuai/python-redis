import conn_redis
import Unit6.DistributedLock as lock
import time

conn = conn_redis.conn


def create_user(login, name):
    """ 创建用户信息散列

    @param string login: 用户名
    @param string name: 昵称
    @return int: 用户id
    """

    l_login = login.lower()
    # 加锁
    lock_id = lock.acquire_lock_with_timeout('user:' + l_login, 1)
    # 加锁不成功，说明给定的用户名已经被其他用户占用了
    if not lock_id:
        return None
    # 如果给定的用户名已经映射到用户id，那么程序就不会再将这个这个用户名分配给其他人
    if conn.hget('users:', l_login):
        lock.release_lock('user:' + l_login, lock_id)
        return None
    # 每个用户都有一个独一无二的id，id是通过对计数器自增操作产生的
    user_id = conn.incr('user:id:')
    pipeline = conn.pipeline(True)
    # 在散列中将小写的用户名映射到id
    pipeline.hset('users:', l_login, user_id)
    # 将用户信息添加到用户对应的散列中
    pipeline.hmset('users:%d' % user_id, {
        'login': l_login,
        'id': user_id,
        'name': name,
        'followers': 0,  # 粉丝
        'following': 0,  # 关注
        'posts': 0,  # 发布数
        'signup': time.time()
    })
    pipeline.execute()
    lock.release_lock('user:' + l_login, lock_id)
    return user_id


def create_status(uid, message, **data):
    """ 创建状态消息散列

    @param int uid: 用户id
    @param string message: 消息
    @param map data: 消息详情
    @return int: 状态id
    """

    pipeline = conn.pipeline(True)
    # 获取用户名
    pipeline.hget('users:%d' % uid, 'login')
    # 获取状态id，id是通过对计数器自增操作产生的
    pipeline.incr('status:id:')
    login, status_id = pipeline.execute()
    # 先验证账号是否存在
    if not login:
        return None
    # 筹备并设置状态消息的各项信息
    data.update({
        'message': message,
        'posted': time.time(),
        'id': status_id,
        'uid': uid,
        'login': login
    })
    # 更新用户发布数
    pipeline.hmset('status:%d' % status_id, data)
    pipeline.hincrby('users:%d' % uid, 'posts')
    pipeline.execute()
    return status_id
