import conn_redis
import bisect
import uuid
import collections
import random
from Unit6 import FileDistribution

conn = conn_redis.conn
COUNTRIES = '''ABW AFG AGO AIA ALA ALB AND ARE ARG ARM ASM ATA ATF ATG AUS AUT AZE BDI BEL BEN BES BFA BGD BGR BHR 
BHS BIH BLM BLR BLZ BMU BOL BRA BRB BRN BTN BVT BWA CAF CAN CCK CHE CHL CHN CIV CMR COD COG COK COL COM CPV CRI CUB 
CUW CXR CYM CYP CZE DEU DJI DMA DNK DOM DZA ECU EGY ERI ESH ESP EST ETH FIN FJI FLK FRA FRO FSM GAB GBR GEO GGY GHA 
GIB GIN GLP GMB GNB GNQ GRC GRD GRL GTM GUF GUM GUY HKG HMD HND HRV HTI HUN IDN IMN IND IOT IRL IRN IRQ ISL ISR ITA 
JAM JEY JOR JPN KAZ KEN KGZ KHM KIR KNA KOR KWT LAO LBN LBR LBY LCA LIE LKA LSO LTU LUX LVA MAC MAP MAR MCO MDA MDG 
MDV MEX MHL MKD MLI MLT MMR MNE MNG MNP MOZ MRT MSR MTQ如S MWI MYS MYT NAM NCL NER NFK NGA NIC NIU NLD NOR NPL NRU NZL 
OMN PAK PAN PCN PER PHL PLW PNG POL PRI PRK PRT PRY PSE PYF QAT REU ROU RUS RWA SAU SDN SEN SGP SGS SHN SJM SLB SLE 
SLV SMR SOM SPM SRB SSD STP SUR SVK SVN SWE SWZ SXM SYC SYR TCA TCD TGO THA TJK TKL TKM TLS TON TTO TUN TUR TUV TWN 
TZA UGA UKR UMI URY USA UZB VAT VCT VEN VGB VIR VNM VUT WLF WSM YEM ZAF ZMB ZWE'''.split()
STATES = {
    'CAN': '''AB BC MB NB NL NS NT NU ON PE QC SK YT'''.split(),
    'USA': '''AA AE AK AL AP AR AS AZ CA CO CT DC DE FL FM GA GU HI IA ID IL IN KS KY LA MA MD ME MH MI MN MO MP MS 
    MT NC ND NE NH NJ NM NV NY OH OK OR PA PR PW RI SC SD TN TX UT VA VI VT WA WI WV WY'''.split(),
}
USERS_PRE_SHARD = 2 ** 6


def get_code(country, state):
    """ 负责将给定的国家和地区转换成编码

    @param string country: 国家
    @param string state: 城市
    @return:
    """

    # 寻找国家或地区对应的偏移量
    c_index = bisect.bisect_left(COUNTRIES, country)
    if c_index > len(COUNTRIES) or COUNTRIES[c_index] != country:
        c_index = -1
    c_index += 1
    # 寻找州对应的偏移量
    s_index = -1
    if state and country in STATES:
        states = STATES[country]
        s_index = bisect.bisect_left(states, state)
        if s_index > len(states) or states[s_index] != state:
            s_index = -1
    s_index += 1
    return str(chr(c_index)) + str(chr(s_index))


def set_location(user_id, country, state):
    """ 存储用户位置

    @param int user_id: 用户id
    @param string country: 国家
    @param string state: 城市
    @return:
    """

    code = get_code(country, state)
    shard_id, position = divmod(user_id, USERS_PRE_SHARD)
    offset = position * 2
    pipe = conn.pipeline(False)
    pipe.setrange('location:%s' % shard_id, offset, code)
    t_key = str(uuid.uuid4())
    pipe.zadd(t_key, {'max': user_id})
    pipe.zunionstore('location:max', [t_key, 'location:max'], aggregate='max')
    pipe.delete(t_key)
    pipe.execute()


def aggregate_location():
    """ 聚合用户信息

    @return:
    """
    countries = collections.defaultdict(int)
    states = collections.defaultdict(lambda: collections.defaultdict(int))
    max_id = int(conn.zscore('location:max', 'max'))
    max_block = max_id // USERS_PRE_SHARD
    for shard_id in range(max_block + 1):
        # 读取分片中的每个块
        for block in FileDistribution.read_blocks('location:%s' % shard_id):
            for offset in range(0, len(block) - 1, 2):
                code = block[offset:offset + 2]
                update_aggregates(countries, states, [code])
    return countries, states


def update_aggregates(countries, states, codes):
    """ 将位置编码转换成国家信息或地区信息

    @param countries:
    @param states:
    @param codes:
    @return:
    """

    for code in codes:
        if len(code) != 2:
            continue
        country = ord(code[0]) - 1
        state = ord(code[1]) - 1
        if country < 0 or country >= len(COUNTRIES):
            continue
        country = COUNTRIES[country]
        countries[country] += 1
        if country not in STATES:
            continue
        if state < 0 or state >= len(STATES[country]):
            continue
        state = STATES[country][state]
        states[country][state] += 1


def aggregate_location_list(user_ids):
    """ 根据给定的用户id进行位置聚合计算

    @param user_ids:
    @return:
    """

    pipe = conn.pipeline(False)
    countries = collections.defaultdict(int)
    states = collections.defaultdict(lambda: collections.defaultdict(int))
    for index, user_id in enumerate(user_ids):
        shard_id, position = divmod(user_id, USERS_PRE_SHARD)
        offset = position * 2
        pipe.substr('location:%s' % shard_id, offset, offset + 1)
        # 每处理1000个请求，程序就会调用之前的辅助函数对聚合数据进行一次更新
        if (index + 1) % 1000 == 0:
            update_aggregates(countries, states, pipe.execute())
    update_aggregates(countries, states, pipe.execute())
    return countries, states


if __name__ == '__main__':
    for i in range(5):
        str_country = random.sample(['CAN', 'USA'], 1)[0]
        str_state = random.sample(STATES[str_country], 1)[0]
        set_location(i, str_country, str_state)
    print(aggregate_location())
