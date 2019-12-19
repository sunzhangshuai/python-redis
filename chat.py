import redis
import time
import json
from Unit6 import DistributedLock

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
conn = redis.Redis(connection_pool=pool)


def create_chat(sender, recipients, message, chat_id=None):
    """ 创建群组
    @param sender:
    @param recipients:
    @param message:
    @param chat_id:
    @return:
    """

    chat_id = chat_id or str(conn.incr("ids:chat:"))
    recipients.append(sender)
    recipientsd = dict((r, 0) for r in recipients)
    pipe = conn.pipeline()
    pipe.zadd("chat:" + chat_id, recipientsd)
    for r in recipients:
        pipe.zadd("seen:" + r, {chat_id: 0})
    pipe.execute()
    return send_message(sender, message, chat_id)


def send_message(sender, message, chat_id):
    """ 发送消息
    @param sender:
    @param message:
    @param chat_id:
    @return:
    """

    identifier = DistributedLock.acquire_lock("chat:" + chat_id)
    if not identifier:
        raise Exception("Could not get the lock")
    try:
        message_id = conn.incr("mid:" + chat_id)
        message_info = {
            "id": message_id,
            "message": message,
            "sender": sender,
            "time": time.time()
        }
        conn.zadd("msg:" + chat_id, {json.dumps(message_info): message_id})
    finally:
        DistributedLock.release_lock("chat:" + chat_id, identifier)
    return chat_id


def fetch_pending_message(recipient):
    """ 读取消息

    @param recipient:
    @return:
    """

    seen = conn.zrange("seen:" + recipient, 0, -1, withscores=True)
    pipe = conn.pipeline(True)
    for chat_id, seen_id in seen:
        pipe.zrangebyscore("msg:" + chat_id, seen_id + 1, "inf")
    chat_info = zip(seen, pipe.execute())
    result = []
    for i, ((chat_id, seen_id), messages) in enumerate(chat_info):
        if not messages:
            continue
        messages[:] = map(json.loads, messages)
        mid = messages[-1]["id"]
        pipe.zadd("chat:" + chat_id, {recipient: mid})
        pipe.zadd("seen:" + recipient, {chat_id: mid})
        pipe.zrange("chat:" + chat_id, 0, 0, withscores=True)
        min_seen = int(pipe.execute()[-1][0][1])
        pipe.zremrangebyscore("msg:" + chat_id, 0, min_seen)
        result.append((chat_id, messages))
    pipe.execute()
    return result


def join_chat(chat_id, user_id):
    """ 加入群组

    @param chat_id:
    @param user_id:
    @return:
    """

    mid = int(conn.get("mid:" + chat_id))
    pipe = conn.pipeline(True)
    pipe.zadd("chat:" + chat_id, {user_id: mid})
    pipe.zadd("seen:" + user_id, {chat_id: mid})
    pipe.execute()


def leave_chat(chat_id, user_id):
    """ 移出群聊

    @param chat_id:
    @param user_id:
    @return:
    """
    pipe = conn.pipeline(True)
    pipe.zrem("chat:" + chat_id, user_id)
    pipe.zrem("seen:" + user_id, chat_id)
    pipe.zcard("chat:" + chat_id)
    if not pipe.execute()[-1]:
        conn.delete("msg:" + chat_id, "mid:" + chat_id)
    else:
        oldest = conn.zrange("chat:" + chat_id, 0, 0, withscores=True)
        conn.zremrangebyscore("msg:" + chat_id, 0, oldest[0][1])


if __name__ == "__main__":
    # create_chat("zhangshuai", ["sun1", "sun2", "sun4"], "小老婆们")
    # send_message("sunsun2", "老公我爱你,么么哒", "1")
    # fetch_pending_message("sun1")
    # fetch_pending_message("sun4")
    # fetch_pending_message("sun2")
    # fetch_pending_message("zhangshuai")
    # join_chat("1", "dasunsun2")
    leave_chat("1", "dasunsun1")
