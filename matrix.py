from itertools import starmap
from operator import mul
from multiprocessing import Manager, cpu_count, Process, Pool
import random
import asyncio
import concurrent.futures

# tasks = []


def mul_atom(a, b):
    # делаем умножение колонки на столбец
    def spec_mul(a, mut_b):
        for row in a:
            yield [sum(starmap(mul, zip(row, col))) for col in mut_b]

    # видоизменяем столбец
    mut_b = tuple(zip(*b))
    res = list(spec_mul(a, mut_b))
    return res

def write_matrix_sync(a, b, offset_a, offset_b, row_size, c):
    """метод выполняет основное вычислительное действие, поэтому его и распараллеливаем"""
    res = mul_atom(a, b)
    for i in range(len(res)):
        row_delta = (i + offset_a) * row_size + offset_b
        for j in range(len(res[0])):
            if res[i][j] and row_delta + j < len(c):
                c[row_delta + j] += res[i][j]

def recurse_mul_sync(a, b, n, offset_a, offset_b, row_size, c):
    """рекурсивный метод, разбивающий матрицу на более мелкие"""
    count_cpu = cpu_count()
    count = row_size // count_cpu if count_cpu < row_size else 2
    print('n', n, count_cpu)
    if n <= count_cpu:
        write_matrix_sync(a, b, offset_a, offset_b, row_size, c)
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
        recurse_mul_sync(a_new_11, b_new_11, n_new, offset_a, offset_b, row_size, c),
        recurse_mul_sync(a_new_12, b_new_21, n_new, offset_a, offset_b, row_size, c),
        # c12 a11b12 + a12b22
        recurse_mul_sync(a_new_11, b_new_12, n_new, offset_a, offset_b + n_new, row_size, c),
        recurse_mul_sync(a_new_12, b_new_22, n_new, offset_a, offset_b + n_new, row_size, c),
        # c21 a21b11 + a22b21
        recurse_mul_sync(a_new_21, b_new_11, n_new, n_new + offset_a, offset_b, row_size, c),
        recurse_mul_sync(a_new_22, b_new_21, n_new, n_new + offset_a, offset_b, row_size, c),
        # c22 a21b12 + a22b22
        recurse_mul_sync(a_new_21, b_new_12, n_new, n_new + offset_a, offset_b + n_new, row_size, c),
        recurse_mul_sync(a_new_22, b_new_22, n_new, n_new + offset_a, offset_b + n_new, row_size, c)

def mul_matrix_sync(a, b):
    """Основной метод для перемножения матриц"""
    if len(a[0]) != len(b):
        return 'Нельзя произвести перемножение, т.к/ матрицы не согласованы. '
    max_size = max(len(a), len(b[0]))
    if len(a) != len(a[0]) or len(a) < max_size:
        a = create_to_sqr(a, max_size)
    if len(b) != len(b[0]) or len(b) < max_size:
        b = create_to_sqr(b, max_size)

    c = list([0 for y in range(max_size * max_size)])
    recurse_mul_sync(a, b, len(a), 0, 0, max_size, c)
    print(a_count)
    return c


def write_matrix(a, b, offset_a, offset_b, row_size, c):
    """метод выполняет основное вычислительное действие, поэтому его и распараллеливаем"""
    res = mul_atom(a, b)
    for i in range(len(res)):
        row_delta = (i + offset_a) * row_size + offset_b
        for j in range(len(res[0])):
            if res[i][j] and row_delta + j < len(c):
                c[row_delta + j] += res[i][j]



async def recurse_mul(a, b, n, offset_a, offset_b, row_size, c, executor):
    """рекурсивный метод, разбивающий матрицу на более мелкие"""
    count_cpu = cpu_count()
    count = row_size // count_cpu if count_cpu < row_size else 2
    # loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    blocking_tasks = []
    if n <= count:
        blocking_tasks.append(loop.run_in_executor(executor, write_matrix, a, b, offset_a, offset_b, row_size, c))
        # write_matrix(a, b, offset_a, offset_b, row_size, c)
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
        recurse_mul(a_new_11, b_new_11, n_new, offset_a, offset_b, row_size, c, executor)
        recurse_mul(a_new_12, b_new_21, n_new, offset_a, offset_b, row_size, c, executor)
        # c12 a11b12 + a12b22
        recurse_mul(a_new_11, b_new_12, n_new, offset_a, offset_b + n_new, row_size, c, executor)
        recurse_mul(a_new_12, b_new_22, n_new, offset_a, offset_b + n_new, row_size, c, executor)
        # c21 a21b11 + a22b21
        recurse_mul(a_new_21, b_new_11, n_new, n_new + offset_a, offset_b, row_size, c, executor)
        recurse_mul(a_new_22, b_new_21, n_new, n_new + offset_a, offset_b, row_size, c, executor)
        # c22 a21b12 + a22b22
        recurse_mul(a_new_21, b_new_12, n_new, n_new + offset_a, offset_b + n_new, row_size, c, executor)
        recurse_mul(a_new_22, b_new_22, n_new, n_new + offset_a, offset_b + n_new, row_size, c, executor)
    try:
        loop.run_until_complete(asyncio.wait(blocking_tasks))
    finally:
        loop.close()

def slice_spec(arr, m, n):
    return [arr[i][m[0]:m[1]] for i in range(n[0], n[1])]


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

def mul_matrix(a, b):
    """Основной метод для перемножения матриц"""
    if len(a[0]) != len(b):
        return 'Нельзя произвести перемножение, т.к/ матрицы не согласованы. '
    max_size = max(len(a), len(b[0]))
    if len(a) != len(a[0]) or len(a) < max_size:
        a = create_to_sqr(a, max_size)
    if len(b) != len(b[0]) or len(b) < max_size:
        b = create_to_sqr(b, max_size)

    c = list([0 for y in range(max_size * max_size)])
    executor = concurrent.futures.ProcessPoolExecutor(
        max_workers=cpu_count(),
    )
    # event_loop = asyncio.get_event_loop()
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)
    try:
        event_loop.run_until_complete(
            recurse_mul(a, b, len(a), 0, 0, max_size, c, executor)
        )
    finally:
        event_loop.close()

    return c


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
