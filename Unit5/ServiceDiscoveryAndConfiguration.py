import conn_redis
import time
import json
import functools
import redis

conn = conn_redis.conn

LAST_CHECKED = None
IS_UNDER_MAINTENANCE = False
CONFIGS = {}
CHECKED = {}
REDIS_CONNECTIONS = {}


def is_under_maintenance():
    """
    判断服务器是否正在进行维护
    """
    global LAST_CHECKED, IS_UNDER_MAINTENANCE
    if LAST_CHECKED < time.time() - 1:
        LAST_CHECKED = time.time()
        IS_UNDER_MAINTENANCE = bool(conn.get('is-under-maintenance'))
    return IS_UNDER_MAINTENANCE


def set_config(service_type, component, conf):
    """
    设置配置值

    @param string service_type: 服务类型
    @param string component: 服务分类
    @param json conf: 配置内容
    @return:
    """
    conn.set(
        'config:%s:%s' % (service_type, component),
        json.dumps(conf)
    )


def get_config(service_type, component, wait=1):
    """
    获取配置

    @param string service_type: 服务类型
    @param string component: 服务分类
    @param int wait: 等待时间
    @return:
    """
    key = 'config:%s:%s' % (service_type, component)
    if CHECKED.get(key) < time.time() - wait:
        CHECKED[key] = time.time()
        config = json.loads(conn.get(key) or '{}')
        config = dict((str(k), config[k]) for k in config)
        old_config = CONFIGS.get(key)
        if config != old_config:
            CONFIGS[key] = config
    return CONFIGS[key]


def redis_connection(component, wait=1):
    """ redis 连接装饰器

    @param string component: 服务分类
    @param int wait: 等待时间
    @return:
    """
    key = 'config:redis:' + component

    def wrapper(function):
        @functools.wraps(function)
        def call(*args, **kwargs):
            old_config = CONFIGS.get(key, object())
            _config = get_config('redis', component, wait)

            config = {}
            for k, v in _config.iteritems():
                config[k.encode('utf-8')] = v
            if config != old_config:
                REDIS_CONNECTIONS[key] = redis.Redis(**config)

            return function(REDIS_CONNECTIONS.get(key), *args, **kwargs)

        return call

    return wrapper
