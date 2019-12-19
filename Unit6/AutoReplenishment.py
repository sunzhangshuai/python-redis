import redis
import conn_redis
import bisect
import uuid

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = conn_redis.conn


def add_update_contact(user, contact):
    """
    添加最近联系人

    @param string user: 用户
    @param string contact: 联系人
    """
    ac_user = "recent:" + user
    pipe = conn.pipeline()
    pipe.lrem(ac_user, 1, contact)
    pipe.lpush(ac_user, contact)
    pipe.ltrim(ac_user, 0, 99)
    pipe.execute()


def remove_contact(user, contact):
    """
    删除最近联系人

    @param string user: 用户
    @param string contact: 联系人
    """
    rem_user = "recent:" + user
    conn.lrem(rem_user, 1, contact)


def fetch_autocomplete_list(user, prefix):
    """
    获取自动补全列表

    @param string user: 用户
    @param string prefix: 前缀
    @return list:
    """
    candidates = conn.lrange("recent:" + user, 0, -1)
    matches = []
    for candidate in candidates:
        if str(candidate).lower().startswith(prefix):
            matches.append(candidate)
    return matches


def find_prefix_range(prefix):
    """
    获取被查找内容的范围

    @param prefix:
    @return:
    """
    valid_characters = "`abcdefghijklmnopqrstuvwxyz{"
    posn = bisect.bisect_left(valid_characters, prefix[-1:])
    suffix = valid_characters[(posn or 1) - 1]
    return prefix[:-1] + suffix + "{", prefix + "{"


def autocomplete_on_prefix(guild, prefix):
    """
    工会前缀匹配通讯录

    @param string guild: 公会号
    @param string prefix: 前缀
    @return:
    """
    items = []
    # 根据给定的前缀计算出查找范围的起点和终点
    start, end = find_prefix_range(prefix)
    identify = str(uuid.uuid4())
    start += identify
    end += identify
    member = "members:" + guild
    # 将范围的起始元素和结束元素添加到有序集合里
    conn.zadd(member, {start: 0, end: 0})
    pipe = conn.pipeline(True)
    while 1:
        try:
            pipe.watch(member)
            # 找到两个被插入元素在有序集合中的排名
            start_index = pipe.zrank(member, start)
            end_index = pipe.zrank(member, end)
            pipe.multi()
            # 删掉插入的元素
            pipe.zrem(member, start, end)
            # 最多只获取10个元素
            erange = min(start_index + 9, end_index - 2)

            pipe.zrange(member, start_index, erange)
            items = pipe.execute()[-1]
            break
        except redis.exceptions.WatchError:
            continue

    return [item for item in items if "{" not in item]


def join_guild(guild, user):
    """
    加入公会

    @param guild:
    @param user:
    @return:
    """
    # 有序集合分值相同，会按照key排序
    conn.zadd("members:" + guild, {user: 0})


def leave_guild(guild, user):
    """
    离开工会

    @param guild:
    @param user:
    @return:
    """
    conn.zrem("members:" + guild, user)


if __name__ == '__main__':
    join_guild('zhangshuai', 'niujie')
    join_guild('zhangshuai', 'niujie1')
    join_guild('zhangshuai', 'niujie2')
    join_guild('zhangshuai', 'niujie3')
    join_guild('zhangshuai', 'niujie4')
    join_guild('zhangshuai', 'niujie5')
    join_guild('zhangshuai', 'niujie6')
    join_guild('zhangshuai', 'niujie7')
    join_guild('zhangshuai', 'niujie8')
    join_guild('zhangshuai', 'niujie9')
    join_guild('zhangshuai', 'niujie10')
    join_guild('zhangshuai', 'niujiea')
    join_guild('zhangshuai', 'niujieb')
    join_guild('zhangshuai', 'aniujieb')
    # print(fetch_autocomplete_list('zhangshuai', 'niu'))
    # print(find_prefix_range('niu'))
    print(autocomplete_on_prefix('zhangshuai', 'niu'))
    # remove_contact('zhangshuai', 'niujie')
