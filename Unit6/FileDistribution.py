import conn_redis
import collections
from Unit6 import Chat
from Unit5 import IpAndCountry
import os
import time
import zlib

conn = conn_redis.conn

aggregates = collections.defaultdict(lambda: collections.defaultdict(int))


def daily_country_aggregate(line):
    """ 一个本地聚合计算回调函数，每天以国家维度对日志进行聚合

    @param str line: 日志行
    """
    if line:
        line = line.split()
        ip = line[0]
        day = line[1]
        country = IpAndCountry.find_city_by_ip(ip)[2]
        aggregates[day][country] += 1
        return
    for day, aggregate in aggregates.items():
        conn.zadd("daily:country:" + day, aggregate)
        del aggregates[day]


def copy_logs_to_redis(path, channel, count=10, limit=2 * 30, quit_when_done=True):
    """ 复制日志到redis

    @param str path: 日志路径
    @param int channel:
    @param int count: 日志分发的数量
    @param limit:
    @param quit_when_done:
    @return:
    """

    byte_in_redis = 0
    waiting = collections.deque()
    Chat.create_chat("source", map(str, range(count)), '', channel)
    count = str(count)
    # 遍历所有日志文件
    for logfile in sorted(os.listdir(path)):
        full_path = os.path.join(path, logfile)
        f_size = os.stat(full_path).st_size
        # 如果程序需要更多空间，那么清除已经处理完毕的文件
        if byte_in_redis + f_size > limit:
            cleaned = _clean(channel, waiting, int(count))
            if cleaned:
                byte_in_redis -= cleaned
            else:
                time.sleep(.25)
        # 将文件上传至redis
        with open(full_path, "rb") as inp:
            block = " "
            while block:
                block = inp.read(2 ** 17)
                conn.append(str(channel) + logfile, block)
        # 提醒监听者，文件已准备就绪
        Chat.send_message("source", logfile, channel)
        # 对本地记录的redis内存占用量相关信息进行更新
        byte_in_redis += f_size
        waiting.append((logfile, f_size))
    # 所有日志文件已经处理完毕，向监听者报告此事
    if quit_when_done:
        Chat.send_message("source", ":done", channel)
    # 在工作完成之后，清理无用的日志文件
    while waiting:
        cleaned = _clean(channel, waiting, int(count))
        if cleaned:
            byte_in_redis -= cleaned
        else:
            time.sleep(.25)


def _clean(channel, waiting, count):
    """ 对redis清理的详细步骤

    @param int channel:
    @param waiting:
    @param int count: 日志分发的数量
    @return:
    """

    while not waiting:
        return 0
    w0 = waiting[0][0]
    if conn.get(str(channel) + w0 + ':done') == count:
        conn.delete(str(channel) + w0 + ':done', channel + w0)
        return waiting.popleft()[1]
    return 0


def process_logs_from_redis(seen_id, callback):
    """ 处理日志文件

    @param string seen_id: 接收者id
    @param callback:
    @return:
    """
    while True:
        # 获取文件列表
        f_data = Chat.fetch_pending_message(seen_id)
        for ch, m_data in f_data:
            for message in m_data:
                logfile = message['message']
                # 所有日志已处理完毕
                if logfile == 'done':
                    return
                elif not logfile:
                    continue
                # 选择一个块读取器
                block_reader = read_blocks
                if str(logfile).endswith('.gz'):
                    block_reader = read_blocks_gz
                # 遍历日志行
                for line in read_lines(ch + logfile, block_reader):
                    # 将日志行传递给回调函数
                    callback(line)
                # 强制刷新聚合数据缓存
                callback(None)
                # 日志已经处理完毕，向文件发送者报告这一信息
                conn.incr(ch + logfile + ':done')
        if not f_data:
            time.sleep(.1)


def read_lines(key, callback_fun):
    """ 数据生成器

    @param key:
    @param callback_fun: 块迭代回调函数
    @return:
    """
    out = ''
    for block in callback_fun(key):
        out += block
        # 查找位于文本最右端的换行符，如果换行符不存在，则rfind返回-1
        pos = out.rfind('\n')
        if pos > 0:
            for line in out[:pos].split('\n'):
                yield line + '\n'
        out = out[pos + 1:]
        # 所有数据块已经处理完毕
        if not block:
            yield out
            break


def read_blocks(key, block_size=2 ** 17):
    """ 获得被读取的数据块

    @param string key:
    @param int block_size: 数据大小
    @return:
    """
    lb = block_size
    pos = 0
    # 尽可能地读取更多数据，直到出现不完整读操作为止
    while lb == block_size:
        # 获取数据块
        block = conn.substr(key, pos, pos + block_size - 1)
        # 为下一次遍历做准备
        yield block
        lb = len(block)
        pos += pos
    yield ''


def read_blocks_gz(key):
    """ 压缩文件生成器

    @param key:
    @return:
    """
    inp = ''
    decoder = None
    for block in read_blocks(key):
        if not decoder:
            inp += block
            try:
                # 分析头信息以便取得被压缩的数据
                if inp[:3] != '\x1f\x8b\x08':
                    raise IOError('invalid gzip data')
                i = 10
                flag = ord(inp[3])
                if flag & 4:
                    i += 2 + ord(inp[i]) + 256 * ord(inp[i + 1])
                if flag & 8:
                    i = inp.index('\0', i) + 1
                if flag & 16:
                    i = inp.index('\0', i) + 1
                if flag & 2:
                    i += 2
                # 程序读取的头信息并不完整
                if i > len(inp):
                    raise IndexError('not enough data')
                else:
                    # 已经找到头信息，准备好相应的解压程序
                    block = inp[i:]
                    inp = None
                    decoder = zlib.decompressobj(-zlib.MAX_WBITS)
            except (IndexError, ValueError):
                continue
        # 所有数据已经处理完毕，向调用者返回最后剩下的数据块
        if not block:
            yield decoder.flush()
        # 向调用者返回解压后的数据块
        yield decoder.decompress(block)
