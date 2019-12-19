import conn_redis
import Unit8.UserAndStatus as status
import Unit6.DistributedLock as lock_util

POSTS_PER_PASS = 1000
conn = conn_redis.conn


def post_status(uid, message, **data):
    """ 发布消息

    @param uid: 发布消息的用户id
    @param message: 消息内容
    @param data: 消息数组
    @return:
    """

    status_id = status.create_status(uid, message, **data)
    if not status_id:
        return None
    posted = conn.hget("status:" + status_id, "posted")
    if not posted:
        return None
    post = {str(id): float(posted)}
    conn.zadd("profile:" + uid, **post)
    syndicate_status(uid, post)
    return status_id


def syndicate_status(uid, post, start=0):
    """ 同步消息给关注的人

    @param uid: 被关注者
    @param post: 被关注者发送的消息id+发布时间
    @param start: 关注者第一位
    @return:
    """

    followers = conn.zrangebyscore("followers:" + uid, start, 'inf', 0, POSTS_PER_PASS, withscores=True)
    pipe = conn.pipeline(False)
    for follower, start in followers:
        pipe.zadd("home:%s" % follower, **post)
        pipe.zremrangebyrank("home:%s" % follower, 0, -POSTS_PER_PASS-1)
    pipe.execute()
    if len(followers) > POSTS_PER_PASS:
        execute_later()


def execute_later():
    pass


def delete_status(uid, status_id):
    """ 删除发布的消息

    @param uid: 发布消息的用户
    @param status_id: 消息id
    @return:
    """

    key = "status%s" % status_id
    lock = lock_util.acquire_lock(key, 1)
    if not lock:
        return None
    if conn.hget(key, 'uid') != str(uid):
        lock_util.release_lock(key, lock)
        return None
    pipe = conn.pipeline(True)
    pipe.delete(key)
    pipe.zrem("profile:" + uid, key)
    pipe.zrem("home:" + uid, key)
    pipe.hincrby("user:%s" % uid, "posts", -1)
    pipe.execute()
    lock_util.release_lock(key, lock)
    return True
