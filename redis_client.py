import redis


def connect_redis():
    # todo: вынести в конфиг файл
    return redis.StrictRedis(host='localhost', port=6379, db=0)


def init_result_matrix(id_req: str, mat_result: list, count: int) -> bool:
    """
    init in redis result matrix 'id + "_result"'  and size this matrix 'id + "_count"'
    """
    rd = connect_redis()
    rd.rpush(id_req + "_result", *mat_result)
    return rd.set(id_req + "_count", count)


def save_el_in_redis(rd, l_name, row_delta, j, i, res):
    """
    создана транзакция для обновления вычисленного значения в redis
    """

    def client_side_incr(pipe):
        current_value = pipe.lindex(l_name, row_delta + j)
        next_value = int(current_value) + res[i][j]
        pipe.multi()
        pipe.lset(l_name, row_delta + j, next_value)

    rd.transaction(client_side_incr, l_name)
