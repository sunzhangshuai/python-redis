import conn_redis
import Unit7.Search as search
import Unit7.SortIndex as sortIndex

AVERAGE_PER_1K = {}


def cpc_to_ecpm(views, clicks, cpc):
    """ 计算cpc广告的ecpm （按千次展示计费的估值）

    @param views: 展示千次数
    @param clicks: 点击次数
    @param cpc: 按点击次数收费的广告 每点击一次的费用
    @return:
    """

    return cpc * clicks / views * 1000


def cpa_to_ecpm(views, actions, cpa):
    """ 计算cpa广告每展示千次收费多少

    @param views: 展示千次数
    @param actions: 执行次数
    @param cpa: 每执行一次收费多少
    @return:
    """

    return cpa * actions / views * 1000


TO_ECPM = {
    'cpc': cpc_to_ecpm,
    'cpa': cpa_to_ecpm,
    'cpm': lambda *args: args[:-1]
}


def index_ad(conn, idx, locations, contents, ad_type, value):
    """ 基于位置和广告内容，为广告增加索引

    @param conn:
    @param idx: 广告id
    @param locations: 广告位置数组
    @param contents: 广告内容
    @param ad_type: 广告类型（cpc，cpa，cpm）
    @param value: 广告费用
    @return:
    """

    pipeline = conn.pipeline(True)
    for location in locations:
        pipeline.sadd('idx:req:' + location, idx)
    words = search.tokenize(contents)
    for word in words:
        pipeline.zadd('idx:' + word, {idx, 0})
    rvalue = TO_ECPM[ad_type](1000, 1, AVERAGE_PER_1K.get(ad_type, 1), value)
    pipeline.hset('vote_type:', idx, ad_type)
    pipeline.zadd('idx:ad:value:', {idx: rvalue})
    pipeline.zadd('ad:base_value:', {idx: value})
    pipeline.sadd('terms:' + idx, *list(words))
    pipeline.execute()


def match_location(pipe, locations):
    """ 基于位置执行广告定向操作的辅助函数

    @param pipe:
    @param locations: 位置
    @return:
    """

    required = ['req:' + loc for loc in locations]
    matched_ads = search.union(pipe, required, ttl=300, execute=False)
    return matched_ads, sortIndex.zintersert(pipe, {matched_ads: 0, 'ad:value': 1}, ttl=300, execute=False)


def finish_scoring(pipe, matched, base, content):
    """ 计算包含了内容匹配附加值的广告eCPM

    @param pipe:
    @param matched: 位置过滤后的广告
    @param base: 广告的基础ecpm
    @param content: 查询内容
    @return:
    """

    bonus_ecpm = {}
    words = search.tokenize(content)
    for word in words:
        word_bonus = sortIndex.zintersert(pipe, {matched: 0, word: 1}, _execute=False)
        bonus_ecpm[word_bonus] = 1
    if bonus_ecpm:
        minimum = sortIndex.zunion(pipe, bonus_ecpm, aggregate='MIN', _execute=False)
        maximum = sortIndex.zunion(pipe, bonus_ecpm, aggregate='MAX', _execute=False)
        return words, sortIndex.zunion(pipe, {base: 1, minimum: .5, maximum: .5}, _execute=False)
    return words, base


def target_ads(conn, locations, content):
    """ 通过位置和关键词附加值实现广告定向操作

    @param conn:
    @param locations:位置
    @param content:查询内容
    @return:
    """

    pipe = conn.pipeline(True)
    matched_ads, base_ecpm = match_location(pipe, locations)
    words, t_ads = finish_scoring(pipe, matched_ads, base_ecpm, content)
    pipe.incr("ads:served:")
    pipe.zrevrange("idx:" + t_ads, 0, 0)
    target_id, target_ad = pipe.execute()[-2:]
    if not target_ad:
        return None, None
    ad_id = target_ad[0]
    record_targeting_result(conn, target_id, ad_id, words)
    return target_id, ad_id


def record_targeting_result(conn, target_id, ad_id, words):
    """ 广告定向操作执行完后对结果的记录

    @param conn:
    @param target_id: 所有广告被浏览的次数
    @param ad_id: 广告id
    @param words: 检索使用到的关键词
    @return:
    """

    pipe = conn.pipeline(True)
    # 找出内容与广告相匹配的单词
    terms = pipe.smembers("terms:" + ad_id)
    matched = list(words & terms)
    # 如果有相匹配的单词出现，就记录他们，并设置15分钟超时时间
    if matched:
        matched_key = "terms:matched:%s" % target_id
        pipe.sadd(matched_key, *matched)
        pipe.expire(matched_key, 900)
    # 为每个种类的广告记录展示次数
    ad_type = conn.hget("vote_type:", ad_id)
    pipe.incr("vote_type:%s:views:" % ad_type)

    for word in matched:
        # 为每个单词记录展示次数
        pipe.zincrby("views:%s" % ad_id, word)
    # 为广告记录展示次数
    pipe.zincrby("views:%s" % ad_id, '')
    # 广告每展示100次，更新它的ecpm
    if not pipe.execute()[-1] % 100:
        update_cpms(conn, ad_id)


def record_click(conn, target_id, ad_id, action=False):
    """ 记录点击次数或者动作执行次数

    @param conn:
    @param target_id: 所有广告的展示次数
    @param ad_id: 广告id
    @param action: 是否是动作执行
    @return:
    """

    pipe = conn.pipeline(True)
    click_key = "click:%s" % ad_id
    matched_key = "terms:matched:%s" % target_id
    ad_type = conn.hget("vote_type:", ad_id)
    # 如果这是一个按照动作计费的广告，并且被匹配的单词仍然存在，刷新超时时间
    if ad_type == "cpa":
        pipe.expire(matched_key, 900)
        if action:
            # 记录动作信息，而不是点击信息
            click_key = "actions:%s" % ad_id
    # 根据广告的类型，记录一个全局的点击/执行动作计数器
    if action and ad_type == "cpa":
        pipe.incr("vote_type:%s:actions" % ad_type)
    else:
        pipe.incr("vote_type:%s:clicks" % ad_type)
    # 为广告以及所有被定向至该广告的单词记录点击次数(或动作)
    matched = list(conn.smembers(matched_key))
    matched.append('')
    for word in matched:
        pipe.zincrby(click_key, word)
    pipe.execute()
    update_cpms(conn, ad_id)


def update_cpms(conn, ad_id):
    """ 更新广告的ecpm 和 广告每个单词的ecpm附加值

    @param conn:
    @param ad_id:
    @return:
    """

    pipeline = conn.pipeline(True)
    # 获取广告的类型和价格，以及广告包含的所有单词
    pipeline.hget('vote_type:', ad_id)
    pipeline.zscore('ad:base_value:', ad_id)
    pipeline.smembers('terms:' + ad_id)
    ad_type, base_value, words = pipeline.execute()
    # 判断广告的ecpm计算方式
    which = 'clicks'
    if ad_type == 'cpa':
        which = 'actions'
    # 获取类型所有广告的展示次数和点击次数/执行次数
    pipeline.get('vote_type:%s:values' % ad_type)
    pipeline.get('vote_type:%s:%s' % (ad_type, which))
    type_views, type_clicks = pipeline.execute()
    # 将广告的点击率或动作执行率重新写入全局字典里面
    AVERAGE_PER_1K[ad_type] = 1000 * int(type_clicks or '1') / int(type_views or '1')
    if ad_type == 'cpm':
        return
    # 获取指定广告的展示次数和点击次数/执行次数
    view_key = 'views:%s' % ad_id
    click_key = '%s:%s' % (which, ad_id)
    to_ecpm = TO_ECPM[ad_type]
    pipeline.zscore(view_key, '')
    pipeline.zscore(click_key, '')
    ad_views, ad_clicks = pipeline.execute()
    # 获取并更新广告的ecpm
    if (ad_clicks or 0) < 1:
        ad_ecpm = conn.zscore('idx:ad:value:', ad_id)
    else:
        ad_ecpm = to_ecpm(ad_views or 1, ad_clicks or 0, base_value)
        pipeline.zadd('idx:ad:value:', {ad_id: ad_ecpm})

    for word in words:
        # 获取单词的展示次数和点击次数
        pipeline.zscore(view_key, word)
        pipeline.zscore(click_key, word)
        word_views, word_click = pipeline.execute()[-2:]
        # 如果单词没有点击过，不更新附加值
        if (word_click or 0) < 1:
            continue
        # 计算单词的ecpm
        word_ecpm = to_ecpm(word_views or 1, word_click or 0, base_value)
        # 计算单词附加值
        bonus = word_ecpm - ad_ecpm
        pipeline.zadd('idx:' + word, ad_id, bonus)
    pipeline.execute()


if __name__ == '__main__':
    conn1 = conn_redis.conn
