import redis
import os

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)


def process_log(path, callback):
    """ 数据恢复

    @param path:
    @param callback:
    @return:
    """

    current_file, offset = conn.mget('progress:ipfind', 'progress:position')

    pipe = conn.pipeline()

    def update_progress():
        pipe.mset({
            'progress:ipfind': current_file,
            'progress:position': offset
        })
        pipe.execute()

    for fname in sorted(os.listdir(path)):
        if current_file is None:
            current_file = fname
            offset = 0

        if fname < current_file:
            continue

        inp = open(os.path.join(path, fname), 'rb')
        if fname == current_file:
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
