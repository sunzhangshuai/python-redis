import conn_redis
import Unit7.Search as search
import uuid


def search_and_zsort(conn, query, idx=None, ttl=30, execute=True, update=1, vote=0, start=0, num=20, desc=True):
    """ 进行搜索并根据更新时间和投票数量进行排序

    @param conn:
    @param query:
    @param idx:
    @param ttl:
    @param execute:
    @param update:
    @param vote:
    @param start:
    @param num:
    @param desc:
    @return:
    """

    if idx and not conn.expire("idx:" + idx, ttl):
        idx = None
    if not idx:
        idx = search.parse_and_search(conn, query, ttl, execute)
    scored_search = {
        idx: 0,
        'sort:updated:': update,
        'sort:votes:': vote
    }
    idx = zintersert(conn, scored_search, ttl)
    pipeline = conn.pipeline() if execute else conn
    pipeline.zcard()
    if desc:
        pipeline.zrevrange(idx, start, start + num - 1)
    else:
        pipeline.zrange(idx, start, start + num - 1)
    result = pipeline.execute()
    return result[0], result[1], idx


def zintersert(conn, scores, ttl=30, **kw):
    """ 计算加权交集

    @param conn:
    @param scores:
    @param ttl:
    @param kw:
    @return:
    """

    return _zset_common(conn, 'zinterscore', dict(scores), ttl, **kw)


def zunion(conn, scores, ttl=30, **kw):
    """ 计算加权并集

    @param conn:
    @param scores:
    @param ttl:
    @param kw:
    @return:
    """

    return _zset_common(conn, 'zunionscore', dict(scores), ttl, **kw)


def _zset_common(conn, method, scores, ttl=30, **kw):
    """ 计算加权交集，并集的公共方法
    @param conn:
    @param method:
    @param scores:
    @param ttl:
    @param kw:
    @return:
    """

    idx = str(uuid.uuid4())
    execute = kw.pop('_execute', True)
    pipeline = conn.pipeline() if execute else conn
    for key in scores.keys():
        scores['idx:' + key] = scores.pop(key)
    getattr(pipeline, method)('idx:' + idx, scores, **kw)
    pipeline.expire('idx:' + idx, ttl)
    if execute:
        pipeline.execute()
    return idx


def string_to_score(string, ignore_case=False):
    """ 将字符串转为分值，只依据6个字符转换

    @param string string:
    @param ignore_case:
    @return:
    """

    if ignore_case:
        string = string.lower()
    pieces = list(map(ord, string[:6]))
    if len(pieces) < 6:
        string_len = len(pieces)
        while string_len < 6:
            string_len += 1
            pieces.append(0)
    score = 0
    for piece in pieces:
        score = score * 256 + piece - 1
    return score * 2 + (len(string) > 6)


if __name__ == '__main__':
    conn1 = conn_redis.conn
    sc = string_to_score('sc')
    sba = string_to_score('sba')
    print(sc > sba)
