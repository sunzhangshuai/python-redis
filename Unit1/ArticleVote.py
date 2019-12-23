import time
import conn_redis

conn = conn_redis.conn
ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432
ARTICLES_PRE_PAGE = 25


def article_vote(user, article):
    """
    对文章投票
    @param string user: 用户
    @param string article: 文章
    """
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time:', article) < cutoff:
        return
    article_id = article.partition(':')[-1]
    if conn.sadd('voted:' + article_id, user):
        conn.zincrby('score:', VOTE_SCORE, article)
        conn.hincrby(article, 'votes', 1)


def post_article(user, title, link):
    """
    发布文章
    @param string user: 用户
    @param string title: 文章标题
    @param string link: 连接
    @return: 文章id
    """
    article_id = str(conn.incr('article_id:'))

    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)
    article_time = time.time()
    article = 'article_id:' + article_id
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': article_time,
        'votes': 1
    })
    conn.zadd('time:', {article: article_time})
    conn.zadd('score:', {article: article_time + VOTE_SCORE})
    return article_id


def get_article(page, order='score:'):
    """
    根据排序获取文章
    @param int page: 页数
    @param string order: 文章来源
    @return: 文章
    """
    start = (page - 1) * ARTICLES_PRE_PAGE
    end = start + ARTICLES_PRE_PAGE - 1
    # 按页获取文章
    article_ids = conn.zrevrange(order, start, end)
    pipeline = conn.pipeline()
    for article_id in article_ids:
        pipeline.hgetall(article_id)
    articles = pipeline.execute()
    # 将文章id放入文章中
    for article_id in article_ids:
        index = article_ids.index(article_id)
        article = articles[index]
        article['id'] = article_id
    return articles


def add_remove_group(article_id, to_add=(), to_remove=()):
    """
    对文章分组
    @param string article_id: 文章id
    @param tuple to_add: 要加入的组
    @param tuple to_remove: 要删除的组
    """
    article = 'article_id:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)


def get_group_articles(group, page, order='score:'):
    """
    分组排序
    @param string group: 组名
    @param int page: 页数
    @param string order: 文章来源
    @return: 文章
    """
    key = order + group
    if not conn.exists(key):
        conn.zinterstore(key, ['group:' + group, order], aggregate='max')
        conn.expire(key, 60)
    return get_article(page, key)


def article_vote_new(user, article_id, vote_type='agree'):
    """
    对文章投票，增加反对投票
    @param string vote_type: 投票类型
    @param string user: 用户
    @param string article_id: 文章id
    """
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time:', article_id) < cutoff:
        return
    article_id = article_id.partition(':')[-1]
    if conn.sadd('voted:' + article_id, user):
        if vote_type == 'agree':
            conn.zincrby('score:', VOTE_SCORE, article_id)
            conn.hincrby(article_id, 'agree_votes', 1)
        else:
            conn.zincrby('score:', VOTE_SCORE * -1, article_id)
            conn.hincrby(article_id, 'against_votes', 1)


def post_article_new(user, title, link):
    """
    发布文章，区分喜欢和不喜欢
    @param string user: 用户
    @param string title: 文章标题
    @param string link: 连接
    @return: 文章id
    """

    article_id = str(conn.incr('article_id:'))

    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    article_time = time.time()

    article = 'article_id:' + article_id
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': article_time,
        'agree_votes': 1,
        'against_votes': 0
    })
    conn.zadd('time:', {article: article_time})
    conn.zadd('score:', {article: article_time + VOTE_SCORE})
    return article_id


if __name__ == '__main__':
    # 1.发布文章
    post_article('user:sunchen', 'redis 实战学习', 'http://www.baidu.com')
    # 2.投票
    # article_vote(redis_conn, 'user:zhangshuai', 'article_id:2')
    # 3.获取排行榜
    article_rank = get_article(1)
    # print(article_rank)
    # 4.对文章分组
    # add_remove_group(redis_conn, '3', to_add=['sb', 'dsb'])
    # add_remove_group(redis_conn, 'article_id:3', to_remove=['sb', 'dsb'])
    # 5.获取dsa分组内的文章排行榜
    # group_articles = get_group_articles(redis_conn, 'dsb', 1)
    # print(group_articles)
    # article_vote_new(redis_conn, 'user:zhangshuai', 'article_id:2')
