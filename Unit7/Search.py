import conn_redis
import re
import uuid

STOP_WORDS = set('''able about across after all almost also am among an and any are as at be because been but by can 
cannot could dear did do does either else ever every for from get got had has have he her hers him his how however if 
in into is it its just least let like likely may me might most must my neither no nor not of off often on only or 
other our own rather said say says she should since so some than that the their them then there these they this tis 
to too twas us wants was we where which when were what while who whom why will with would yet you your'''.split())
WORDS_RE = re.compile("[a-z']{2,}")
QUERY_RE = re.compile("[+-]?[a-z']{2,}")


def tokenize(doc_content):
    """ 单词标记化

    @param string doc_content: 文章内容
    @return:
    """

    words = set()
    for match in WORDS_RE.finditer(doc_content.lower()):
        word = match.group().strip("'")
        if len(word) >= 2:
            words.add(word)
    return words - STOP_WORDS


def index_document(conn, docId, doc_content):
    """ 为文章增加索引

    @param conn:
    @param string docId: 文章id
    @param string doc_content: 文章内容
    @return:
    """

    words = tokenize(doc_content)
    pipeline = conn.pipeline(True)
    for word in words:
        pipeline.sadd("idx:" + word, docId)
    return len(pipeline.execute())


def _set_common(conn, method, names, ttl=30, execute=True):
    """ 计算交集，并集，差集的公共方法

    @param conn:
    @param method: 计算类别
    @param names: 搜索的单词
    @param ttl: 查询到的结果集key的过期时间
    @param execute: 是否执行流水线事务
    @return:
    """

    idx = str(uuid.uuid4())
    pipeline = conn.pipeline(True) if execute else conn
    names = ["idx:" + name for name in names]
    getattr(pipeline, method)("idx:" + idx, *names)
    pipeline.expire("idx:" + idx, ttl)
    if execute:
        pipeline.execute()
    return idx


def intersect(conn, items, ttl, execute):
    """  查询包含items里所有单词的文章

    @param conn:
    @param items: 单词集合
    @param ttl:
    @param execute:
    @return:
    """

    return _set_common(conn, "sinterstore", items, ttl, execute)


def union(conn, items, ttl, execute):
    """ 包含这个或者包含那个的文章集合

    @param conn:
    @param items:
    @param ttl:
    @param execute:
    @return:
    """

    return _set_common(conn, "sunionstore", items, ttl, execute)


def difference(conn, items, ttl, execute):
    """ 包含这个不包含那个的文章集合

    @param conn:
    @param items:
    @param ttl:
    @param execute:
    @return:
    """

    return _set_common(conn, "sdiffstore", items, ttl, execute)


def parse(query):
    """ 对搜索内容进行语法解析：【差【交【并】，【并】】】

    @param string query:
    @return:
    """

    # 这个集合用于存储不需要的单词
    unwanted = set()
    # 这个列表用于存储需要执行交集运算的单词
    all_want = []
    # 这个集合用于存储目前已发现的同义词
    current = set()
    for match in QUERY_RE.finditer(query.lower()):
        word = match.group()
        prefix = word[:1]
        if prefix in '-+':
            word = word[1:]
        else:
            prefix = None
        word = word.strip("'")
        if len(word) < 2 or word in STOP_WORDS:
            continue
        if prefix == '-':
            unwanted.add(word)
            continue
        if current and prefix is None:
            all_want.append(current)
            current = set()
        current.add(word)
    if current:
        all_want.append(current)
    return all_want, list(unwanted)


def parse_and_search(conn, query, ttl=30, execute=True):
    """ 解析查询语句，并返回查询结果

    @param conn:
    @param query:
    @param ttl:
    @param execute:
    @return:
    """

    all_want, unwanted = parse(query)
    if not all_want:
        return None
    to_intersect = []
    for syn in all_want:
        if len(syn) > 1:
            to_intersect.append(union(conn, syn, ttl, execute))
        else:
            to_intersect.append(syn.pop())
    if len(to_intersect) > 1:
        intersect_result = intersect(conn, to_intersect, ttl, execute)
    else:
        intersect_result = to_intersect[0]
    if unwanted:
        unwanted.insert(0, intersect_result)
        return difference(conn, unwanted, ttl, execute)
    return intersect_result


def search_and_sort(conn, query, idx=None, sort='-updated', ttl=300, execute=True, start=0, num=20):
    """ 为搜索结果进行排序

    @param conn:
    @param query:
    @param idx:
    @param sort:
    @param ttl:
    @param execute:
    @param start:
    @param num:
    @return:
    """

    desc = sort.startswith('-')
    sort = sort.lstrip('-')
    by = "kb:doc:*->" + sort
    alpha = sort not in ('updated', 'id', 'created')
    if idx and not conn.expire("idx:" + idx, ttl):
        idx = None
    if not idx:
        idx = parse_and_search(conn, query, ttl, execute)
    pipe = conn.pipeline()
    pipe.scard("idx:" + idx)
    pipe.sort("idx:" + idx, by=by, alpha=alpha, desc=desc, start=start, num=num)
    results = pipe.execute()
    return results[0], results[1], idx


if __name__ == "__main__":
    conn1 = conn_redis.conn
    # content = '''In order to construct our SETs of documents, we must first examine our documents for words. The
    # process of extracting words from documents is known as parsing and tokenization; we are producing a set of tokens
    # (or words) that identify the document. '''
    # index_document("docA", content)
    # content = "aa bb ccc order"
    # index_document("docB", content)
    # idx = parse_and_search("order to -aa", 30, True)
    # print(conn.smembers("idx:" + idx))
    pass
