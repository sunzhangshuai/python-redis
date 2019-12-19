import conn_redis

conn = conn_redis.conn


def get_status_message(uid, timeline='home:', page=1, page_size=30):
    """ 从时间线中获取给定页数的最新状态消息

    @param int uid: 用户id
    @param string timeline: 时间线类型
    @param int page: 页数
    @param int page_size: 每页数量
    @return list: 状态消息列表`
    """

    statuses = conn.zrevrange('%s%s' % (timeline, uid), page_size * (page - 1), page_size * page - 1)
    pipeline = conn.pipeline(True)
    for status_id in statuses:
        pipeline.hmget('status:%d' % status_id)
    return filter(None, pipeline.execute())
