import redis


def connect_redis():
    return redis.StrictRedis(host='localhost', port=6379, db=0)


def init_result_matrix(id: str, mat_result: list, count: int) -> bool:
    """
    init in redis result matrix 'id + "_result"'  and size this matrix 'id + "_count"'
    """
    rd = connect_redis()
    print("in redis mtx  ", mat_result)
    rd.rpush(id + "_result", *mat_result)
    print("in redis count ", count)
    return rd.set(id + "_count", count)
