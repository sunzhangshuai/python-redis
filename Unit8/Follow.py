import conn_redis
import time

HOME_TIMELINE_SIZE = 1000
conn = conn_redis.conn

conn.close()


def follow_user(uid, other_uid):
    """ 关注用户

    @param uid: 关注者
    @param other_uid: 被关注者
    @return: 关注是否成功
    """

    following_key = 'following:' + uid
    followers_key = 'followers:' + other_uid
    # 如果用户已经关注了被关注者，直接返回
    if conn.zscore(following_key, other_uid):
        return None
    now_time = time.time()
    pipe = conn.pipeline(True)
    # 1.用户增加被关注人信息
    pipe.zadd(following_key, {other_uid: now_time})
    # 2.新增被关注者的粉丝信息
    pipe.zadd(followers_key, {uid: now_time})
    # 3. 获取被关注者最新的1000条消息
    pipe.zrevrange('profile:' + other_uid, 0, HOME_TIMELINE_SIZE - 1, withscores=True)
    following, followers, status_and_score = pipe.execute()[-3:]
    # 4.增加用户的关注人数量
    pipe.hincrby('users:' + uid, 'following', int(following))
    # 5.增加被关注人粉丝数量
    pipe.hincrby('users:' + other_uid, 'followers', int(followers))
    if status_and_score:
        pipe.zadd('home:' + uid, dict(status_and_score))
    pipe.zremrangebyrank('home:' + uid, 0, -HOME_TIMELINE_SIZE - 1)
    pipe.execute()
    return True


def unfollow_user(uid, other_uid):
    """ 取消关注

    @param uid: 关注者
    @param other_uid: 被关注者
    @return: 取消是否成功
    """

    following_key = 'following:' + uid
    followers_key = 'followers:' + other_uid
    # 如果用户已经不关注了被关注者，直接返回
    if not conn.zscore(following_key, other_uid):
        return None
    pipe = conn.pipeline(True)
    # 1.用户删除该关注人
    pipe.zrem(following_key, other_uid)
    # 2.被关注人删掉该粉丝
    pipe.zrem(followers_key, uid)
    # 3.获取被删除人的所有消息
    pipe.zrevrange('profile:' + other_uid, 0, HOME_TIMELINE_SIZE - 1)
    following, followers, statuses = pipe.execute()[-3:]
    # 4.减少用户的关注数量
    pipe.hincrby('users:' + uid, 'following', -int(following))
    # 5.减少被关注者的粉丝数
    pipe.hincrby('users:' + other_uid, 'followers', -int(followers))
    if statuses:
        pipe.zrem('home:' + uid, *list(statuses))
    pipe.execute()
    return True
