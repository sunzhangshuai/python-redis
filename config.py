import redis
import time
import json

LAST_CHECKED = None
IS_UNDER_MAINTENANCE = False
CONFIGS = {}
CHECKED = {}

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)


def is_under_maintenance():
    """ 判断是否正在维护

    @return:
    """

    global LAST_CHECKED, IS_UNDER_MAINTENANCE
    if LAST_CHECKED < time.time() - 1:
        LAST_CHECKED = time.time()
        IS_UNDER_MAINTENANCE = bool(conn.get("is-under-maintenance"))

    return IS_UNDER_MAINTENANCE


def set_config(str_type, component, config):
    """ 存储组件配置信息

    @param str_type:
    @param component:
    @param config:
    @return:
    """

    conn.set("config:%s:%s" % (str_type, component), json.dumps(config))


def get_config(str_type, component, wait=1):
    """ 获取组件配置信息123
    
    @param str_type: 
    @param component: 
    @param wait: 
    @return:
    """

    key = "config:%s:%s" % (str_type, component)
    if CHECKED.get(key) < time.time() - wait:
        config = json.loads(conn.get(key))
        config = dict((str(k), config[k]) for k in config)
        CONFIGS[key] = config
        CHECKED[key] = time.time()
    return CONFIGS.get(key)
