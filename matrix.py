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
    print(a)
    print(b)

    def spec_mul(a, mut_b):
        for row in a:
            yield [sum(starmap(mul, zip(row, col))) for col in mut_b]

    # видоизменяем столбец
    mut_b = tuple(zip(*b))
    res = list(spec_mul(a, mut_b))
    print(res)
    return res


def write_matrix_sync(a, b, offset_a, offset_b, row_size, c):
    """метод выполняет основное вычислительное действие, поэтому его и распараллеливаем"""
    res = mul_atom(a, b)
    for i in range(len(res)):
        row_delta = (i + offset_a) * row_size + offset_b
        for j in range(len(res[0])):
            if res[i][j] and row_delta + j < len(c):
                c[row_delta + j] += res[i][j]


def mul_matrix_sync(id, a, b):
    """Основной метод для перемножения матриц"""
    if len(a[0]) != len(b):
        return 'Нельзя произвести перемножение, т.к/ матрицы не согласованы. '
    max_size = len(a) * len(a)
    # print("mul_matrix_sync ", max_size, len(a), len(a[0]), len(b), len(b[0]))
    # if len(a) != len(a[0]) or len(a) < max_size:
    #     a = create_to_sqr(a, max_size)
    # if len(b) != len(b[0]) or len(b) < max_size:
    #     b = create_to_sqr(b, max_size)

    c = list([0 for y in range(max_size)])
    print("max_size ", max_size)
    # count = int(math.ceil(((2 ** len(a)) / (2 ** ATOM_SIZE_MATRIX/2))))
    if len(a) <= ATOM_SIZE_MATRIX:
        count = 1
    else:
        count = int(2 ** (len(a) / ATOM_SIZE_MATRIX + 1))
    print("count ", count)
    res = redis_client.init_result_matrix(id, c, count)
    print("init result matrix", res)
    if res:
        send_to_worker(id, a, b, len(a), 0, 0, max_size, 1)
    return c


def send_to_worker(id, a, b, n, offset_a, offset_b, max_size, size):
    packet = {"id": id, "left": a, "right": b, "n": n, "offset_a": offset_a, "offset_b": offset_b, "max_size": max_size,
              "size": size}
    msg = json.dumps(packet)
    rbmq_client.send_msg(msg)


def slice_spec(arr, m, n):
    print("slice_spec ", arr, m, n)
    # return [arr[i][m[0]:m[1]] for i in range(n[0], n[1])]
    res = []
    for i in range(n[0], n[1]):
        print(i)
        res.append(arr[i][m[0]:m[1]])
    print("res ", res)
    return res


def create_to_sqr(a, size):
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


def create_matr(n):
    a = [[0 for x in range(n)] for y in range(n)]
    for i in range(n):
        x = a[i]
        for j in range(n):
            x[j] = random.Random().randint(1, 9)
    return a


def mul_on_numpy(a, b):
    import numpy
    a_nm = numpy.matrix(a)
    b_nm = numpy.matrix(b)
    return a_nm * b_nm
