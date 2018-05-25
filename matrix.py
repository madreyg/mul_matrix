import json
from itertools import starmap
from operator import mul
import random
import redis

# tasks = []
import pika

ATOM_SIZE_MATRIX = 2


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


def connect_redis():
    return redis.StrictRedis(host='localhost', port=6379, db=0)


def mul_matrix_sync(id, a, b):
    """Основной метод для перемножения матриц"""
    if len(a[0]) != len(b):
        return 'Нельзя произвести перемножение, т.к/ матрицы не согласованы. '
    max_size = max(len(a), len(b[0]))
    if len(a) != len(a[0]) or len(a) < max_size:
        a = create_to_sqr(a, max_size)
    if len(b) != len(b[0]) or len(b) < max_size:
        b = create_to_sqr(b, max_size)

    c = list([0 for y in range(max_size * max_size)])
    print(c)
    rd = connect_redis()
    rd.rpush(id + "_result", *[1, 2, 3, 4])
    print("olololo ", rd.lpop(id + "_result"))
    print("olololo ", rd.lindex(id + "_result", 2))
    print("olololo ", rd.lindex(id + "_result", 1))
    print("olololo ", rd.lindex(id + "_result", 0))
    print("olololo ", rd.lindex(id + "_result", -1))
    print("olololo ", rd.lindex(id + "_result", -2))
    print("olololo ", rd.lindex(id + "_result", -3))

    rd.set(id + "_count", int((2 ** (len(a) / 2)) / (2 ** (ATOM_SIZE_MATRIX / 2))))
    send_rbmq(id, a, b, len(a), 0, 0, max_size)
    return c


def send_rbmq(id, a, b, n, offset_a, offset_b, max_size):
    packet = {"id": id, "left": a, "right": b, "n": n, "offset_a": offset_a, "offset_b": offset_b, "max_size": max_size}
    msg = json.dumps(packet)
    send_msg(msg)


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


def send_msg(msg):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost', port=32774))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    message = msg
    channel.basic_publish(exchange='',
                          routing_key='task_queue',
                          body=message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))
    print(" [x] Sent %r" % (message,))
    connection.close()
