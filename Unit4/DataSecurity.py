import conn_redis
import os

conn = conn_redis.conn


def process_log(path, callback):
    """
    数据恢复
    @param string path: 日志路径
    @param function callback: 回调方法
    """
    current_file, offset = conn.mget('progress:ipfind', 'progress:position')
    pipe = conn.pipeline()

    def update_progress():
        pipe.mset({
            'progress:ipfind': current_file,
            'progress:position': offset
        })
        pipe.execute()
    for f_name in sorted(os.listdir(path)):
        if current_file is None:
            current_file = f_name
            offset = 0
        if f_name < current_file:
            continue
        inp = open(os.path.join(path, f_name), 'rb')
        if f_name == current_file:
            inp.seek(int(offset))
        else:
            offset = 0
        for lno, line in enumerate(inp):
            callback(pipe, line)
            offset = int(offset) + len(line)
            if not (lno + 1) % 1000:
                update_progress()
        update_progress()
        current_file = None
        inp.close()
    pass


if __name__ == '__main__':
    pass
# process_log('/data/logs/service_transaction', '')
