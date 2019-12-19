import conn_redis
import uuid
import Unit7.SortIndex as sort

conn = conn_redis.conn


def add_jobs(job_id, required_skills):
    """ 将职位所需的技能全部添加到职位所在集合中

    @param job_id: 工作id
    @param required_skills:技能
    @return:
    """

    conn.sadd("job:" + job_id, *required_skills)


def is_qualified(job_id, candidate_skills):
    """ 根据技能找工作

    @param job_id:工作id
    @param candidate_skills:候选技能
    @return:
    """

    item = str(uuid.uuid4())
    pipe = conn.pipeline()
    pipe.sadd("item:" + item, *candidate_skills)
    pipe.expire("item:" + item)
    pipe.sdiff("job:"+job_id, "item:"+item)
    return not pipe.execute()[-1]


def index_job(job_id, skills):
    """ 根据所需技能对职位进行索引

    @param job_id:工作id
    @param skills:技能
    @return:
    """

    pipe = conn.pipeline(True)
    for skill in skills:
        pipe.sadd("idx:skill:" + skill, job_id)
    pipe.zadd("idx:required:job", {job_id: len(set(skills))})
    pipe.execute()


def find_jobs(candidate_skills):
    """ 找出求职者能够胜任的所有工作

    @param candidate_skills:候选人技能
    @return:
    """

    pipe = conn.pipeline(True)
    skills = {}
    for skill in candidate_skills:
        skills["skill:" + skill] = 1
    job_union = sort.zunion(pipe, skills)
    final_result = sort.zintersert(pipe, {job_union: -1, "idx:required:job": 1})
    pipe.execute()
    return conn.zrangebyscore("idx:" + final_result, 0, 0)
