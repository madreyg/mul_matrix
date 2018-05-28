import json
from itertools import starmap
from operator import mul
import random
import redis_client
import rbmq_client

ATOM_SIZE_MATRIX = 2


def mul_atom(a: list, b: list) -> list:
    """
    делаем умножение колонки на столбец
    """

    def spec_mul(mtx, mut_mtx):
        for row in mtx:
            yield [sum(starmap(mul, zip(row, col))) for col in mut_mtx]

    # видоизменяем столбец
    mut_b = tuple(zip(*b))
    res = list(spec_mul(a, mut_b))
    print(res)
    return res


def mul_matrix(id_req, a, b):
    """Основной метод для перемножения матриц"""
    if len(a[0]) != len(b):
        return 'Нельзя произвести перемножение, т.к/ матрицы не согласованы. '
    max_size = len(a) * len(a)
    # todo: требуется для upgrade алгоритма
    # print("mul_matrix_sync ", max_size, len(a), len(a[0]), len(b), len(b[0]))
    # if len(a) != len(a[0]) or len(a) < max_size:
    #     a = create_to_sqr(a, max_size)
    # if len(b) != len(b[0]) or len(b) < max_size:
    #     b = create_to_sqr(b, max_size)

    c = list([0 for _ in range(max_size)])
    if len(a) <= ATOM_SIZE_MATRIX:
        count = 1
    else:
        count = int(2 ** (len(a) / ATOM_SIZE_MATRIX + 1))
    res = redis_client.init_result_matrix(id_req, c, count)
    if res:
        send_to_worker(id_req, a, b, len(a), 0, 0, max_size, 1)
    return c


def send_to_worker(id_req, a, b, n, offset_a, offset_b, max_size, size):
    packet = {"id": id_req, "left": a, "right": b, "n": n, "offset_a": offset_a, "offset_b": offset_b,
              "max_size": max_size, "size": size}
    msg = json.dumps(packet)
    rbmq_client.send_msg(msg)


def slice_spec(arr, m, n):
    return [arr[i][m[0]:m[1]] for i in range(n[0], n[1])]


def create_to_sqr(a, size):
    """
    метод для улучшения алгоритма для дополнения матриц до квадратных
    """
    if len(a) < len(a[0]) or len(a) < size:
        for i in range(len(a[0]) - len(a)):
            a.append([0] * len(a[0]))
    if len(a) > len(a[0]) or len(a[0]) < size:
        i = len(a) - len(a[0]) + (size - len(a))
        for row in a:
            row.extend([0] * i)
    if len(a) < size:
        a.append([0] * size)
    return a


def create_mtx(n: int) -> list:
    a = [[0 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        x = a[i]
        for j in range(n):
            x[j] = random.Random().randint(1, 9)
    return a


def write_matrix(id_req, a, b, offset_a, offset_b, row_size, size):
    """метод выполняет основное вычислительное действие, поэтому его и распараллеливаем"""

    rd = redis_client.connect_redis()
    res = mul_atom(a, b)
    l_name = id_req + "_result"
    for i in range(len(res)):
        row_delta = round((i + offset_a) * (row_size + size * ATOM_SIZE_MATRIX) + offset_b)
        for j in range(len(res[0])):
            if res[i][j]:
                redis_client.save_el_in_redis(rd, l_name, row_delta, i, j, res)
    return rd.decr(id_req + "_count")


def recurse_mul(id_req, a, b, n, offset_a, offset_b, row_size, size):
    """рекурсивный метод, разбивающий матрицу на более мелкие"""
    if n <= ATOM_SIZE_MATRIX:
        write_matrix(id_req, a, b, offset_a, offset_b, n, size)
    else:
        n_new = n // 2
        a_new_11 = slice_spec(a, [0, n_new], [0, n_new])
        b_new_11 = slice_spec(b, [0, n_new], [0, n_new])
        a_new_12 = slice_spec(a, [n_new, n], [0, n_new])
        b_new_12 = slice_spec(b, [n_new, n], [0, n_new])
        a_new_21 = slice_spec(a, [0, n_new], [n_new, n])
        b_new_21 = slice_spec(b, [0, n_new], [n_new, n])
        a_new_22 = slice_spec(a, [n_new, n], [n_new, n])
        b_new_22 = slice_spec(b, [n_new, n], [n_new, n])
        # c11 a11b11 + a12b21
        send_to_worker(id_req, a_new_11, b_new_11, n_new, offset_a, offset_b, row_size, size)
        send_to_worker(id_req, a_new_12, b_new_21, n_new, offset_a, offset_b, row_size, size),
        # c12 a11b12 + a12b22
        send_to_worker(id_req, a_new_11, b_new_12, n_new, offset_a, offset_b + n_new, row_size, size),
        send_to_worker(id_req, a_new_12, b_new_22, n_new, offset_a, offset_b + n_new, row_size, size),
        # c21 a21b11 + a22b21
        send_to_worker(id_req, a_new_21, b_new_11, n_new, n_new + offset_a, offset_b, row_size, size),
        send_to_worker(id_req, a_new_22, b_new_21, n_new, n_new + offset_a, offset_b, row_size, size),
        # c22 a21b12 + a22b22
        send_to_worker(id_req, a_new_21, b_new_12, n_new, n_new + offset_a, offset_b + n_new, row_size, size),
        send_to_worker(id_req, a_new_22, b_new_22, n_new, n_new + offset_a, offset_b + n_new, row_size, size)


def mul_on_numpy(a, b):
    """
    метод для проверки результата
    """
    import numpy
    a_nm = numpy.matrix(a)
    b_nm = numpy.matrix(b)
    return a_nm * b_nm
