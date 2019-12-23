import conn_redis
import csv
import json
import Unit9.FragmentStructure as fragment

conn = conn_redis.conn


def ip_to_score(ip_address):
    """ 将ip转换为分值

    @param string ip_address:
    @return:
    """
    score = 0
    for v in ip_address.split('.'):
        if "/" in v:
            v = v.split("/")[0]
        score = score * 256 + int(v)
    return score


def import_ips_to_redis():
    """ 导入ip和城市的映射关系

    @return:
    """

    filename = "./GeoLiteCity-Blocks.csv"
    csv_file = csv.reader(open(filename, "r"))
    for count, row in enumerate(csv_file):
        start_ip = row[0] if row else ""
        if "i" in start_ip:
            continue
        if "." in start_ip:
            start_ip = ip_to_score(start_ip)
        elif start_ip.isdigit():
            start_ip = int(start_ip)
        else:
            continue
        city_id = row[1] + "_" + str(count)
        conn.zadd("ip2cityid:", {city_id: start_ip})


def import_cities_to_redis():
    """ 导入城市详情

    @return:
    """

    filename = "./GeoLiteCity-Locations.csv"
    csv_file = csv.reader(open(filename, "r", encoding="gbk"))
    for count, row in enumerate(csv_file):
        if len(row) < 4 or not str(row[0]).isdigit():
            continue
        city_id = row[0]
        country = row[5]
        region = row[3]
        city = row[10]
        fragment.shard_hset("cityId2city:", 'x' + city_id, json.dumps([city, region, country]), 120000, 500)
        conn.hset("cityId2city:", city_id, json.dumps([city, region, country]))


def find_city_by_ip(ip_address):
    """ 根据ip查找城市信息

    @param ip_address:
    @return:
    """

    if isinstance(ip_address, str):
        ip_address = ip_to_score(ip_address)
    city_id = conn.zrevrangebyscore("ip2cityid:", ip_address, 0, start=0, num=1)
    city_id = city_id[0]

    if not city_id:
        return None

    city_id = str(city_id).split("_")[0]
    return json.loads(conn.hget("cityid2city:", city_id))


if __name__ == "__main__":
    # import_ips_to_redis()
    import_cities_to_redis()
    # print(find_city_by_ip("192.128.1.1"))
