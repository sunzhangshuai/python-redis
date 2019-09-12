import redis
import bisect
import uuid

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)


def add_update_contact(user, contact):
    """ 添加最近联系人

    :param user:
    :param contact:
    :return:
    """

    ac_user = "recent:" + user
    pipe = conn.pipeline()
    pipe.lrem(ac_user, contact)
    pipe.lpush(ac_user, contact)
    pipe.ltrim(ac_user, 0, 99)
    pipe.execute()


def remove_contact(user, contact):
    """ 删除最近联系人

    :param user:
    :param str contact:
    :return:
    """

    rem_user = "recent:" + user
    conn.lrem(rem_user, 1, contact)


def fetch_autocomplete_list(user, prefix):
    """ 获取自动补全列表

    :param user:
    :param prefix:
    :return:
    """

    candidates = conn.lrange("recent:" + user, 0, -1)
    matches = []
    for candidate in candidates:
        if str(candidate).lower().startswith(prefix):
            matches.append(candidate)
    return matches


def find_prefix_range(prefix):
    """ 获取被查找内容的范围

    :param prefix:
    :return:
    """

    valid_characters = "`abcdefghijklmnopqrstuvwxyz{"
    posn = bisect.bisect_left(valid_characters, prefix[-1:])
    suffix = valid_characters[(posn or 1) - 1]
    return prefix[:-2] + suffix + "{", prefix + "{"


def autocomplete_on_prefix(guild, prefix):
    """ 工会前缀匹配通讯录

    :param guild: 公会号
    :param prefix: 前缀
    :return:
    """

    items = []
    start, end = find_prefix_range(prefix)
    identify = str(uuid.uuid4())
    start += identify
    end += identify
    member = "members:" + guild
    conn.zadd(member, {start: 0, end: 0})
    pipe = conn.pipeline(True)
    while 1:
        try:
            pipe.watch(member)

            sindex = pipe.zrank(member, start)
            eindex = pipe.zrank(member, end)
            pipe.multi()

            pipe.zrem(member, start, end)
            erange = min(sindex + 9, eindex - 2)
            pipe.zrange(member, sindex, erange)
            items = pipe.execute()[-1]
            break
        except redis.exceptions.WatchError:
            continue

    return [item for item in items if "{" not in item]


def join_guild(guild, user):
    """ 加入公会

    :param guild:
    :param user:
    :return:
    """

    conn.zadd("members:" + guild, {user: 0})


def leave_guild(guild, user):
    """ 离开工会

    :param guild:
    :param user:
    :return:
    """

    conn.zrem("members:" + guild, user)




