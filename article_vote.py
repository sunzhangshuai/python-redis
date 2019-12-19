import time
import redis

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432
ARTICLES_PRE_PAGE = 25

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
redis_conn = redis.Redis(connection_pool=pool)


def article_vote(conn, user, article):
    """ 对文章投票

    @param conn:
    @param user:
    @param article:
    @return:
    """
    cutoff = time.time() - ONE_WEEK_IN_SECONDS

    if conn.zscore('time:', article) < cutoff:
        return

    article_id = article.partition(':')[-1]

    if conn.sadd('voted:' + article_id, user):
        conn.zincrby('score:', VOTE_SCORE, article)
        conn.hincrby(article, 'votes', 1)


def post_article(conn, user, title, link):
    """ 发布文章

    @param conn:
    @param user:
    @param title:
    @param link:
    @return:
    """

    article_id = str(conn.incr('article:'))

    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    article_time = time.time()

    article = 'article:' + article_id
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


def get_article(conn, page, order='score:'):
    """ 根据排序获取文章

    @param conn:
    @param page:
    @param order:
    @return:
    """

    start = (page - 1) * ARTICLES_PRE_PAGE
    end = start + ARTICLES_PRE_PAGE - 1

    article_ids = conn.zrevrange(order, start, end)

    pipeline = conn.pipeline()
    for id in article_ids:
        pipeline.hgetall(id)
    articles = pipeline.execute()

    for id in article_ids:
        index = article_ids.index(id)
        article = articles[index]
        article['id'] = id

    return articles


def add_remove_group(conn, article_id, to_add=[], to_remove=[]):
    """ 对文章分组

    @param conn:
    @param article_id:
    @param to_add:
    @param to_remove:
    @return:
    """

    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)


def get_group_articles(conn, group, page, order='score:'):
    """ 分组排序

    @param conn:
    @param group:
    @param page:
    @param order:
    @return:
    """

    key = order + group

    if not conn.exists(key):
        conn.zinterstore(key, ['group:' + group, order], aggregate='max')
        conn.expire(key, 60)

    return get_article(conn, page, key)


def article_vote_new(conn, user, article, type="agree"):
    """ 对文章投票

    @param conn:
    @param user:
    @param article:
    @return:
    """
    cutoff = time.time() - ONE_WEEK_IN_SECONDS

    if conn.zscore('time:', article) < cutoff:
        return

    article_id = article.partition(':')[-1]

    if conn.sadd('voted:' + article_id, user):

        if type == "agree":
            conn.zincrby('score:', VOTE_SCORE, article)
            conn.hincrby(article, 'agree_votes', 1)
        else:
            conn.zincrby('score:', VOTE_SCORE * -1, article)
            conn.hincrby(article, 'against_votes', 1)


def post_article_new(conn, user, title, link):
    """ 发布文章

    @param conn:
    @param user:
    @param title:
    @param link:
    @return:
    """

    article_id = str(conn.incr('article:'))

    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    article_time = time.time()

    article = 'article:' + article_id
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
    post_article(redis_conn, 'user:sunchen', 'redis 实战学习', 'http://www.baidu.com')
    # 2.投票
    # article_vote(redis_conn, 'user:zhangshuai', 'article:2')
    # 3.获取排行榜
    article_rank = get_article(redis_conn, 1)
    # print(article_rank)
    # 4.对文章分组
    # add_remove_group(redis_conn, '3', to_add=['sb', 'dsb'])
    # add_remove_group(redis_conn, 'article:3', to_remove=['sb', 'dsb'])
    # 5.获取dsa分组内的文章排行榜
    # group_articles = get_group_articles(redis_conn, 'dsb', 1)
    # print(group_articles)
    # article_vote_new(redis_conn, 'user:zhangshuai', 'article:2')
