from itertools import starmap

import pika
import time
import json
from operator import mul

import redis

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost', port=32770))
channel = connection.channel()

channel.queue_declare(queue='matrix_queue', durable=True)


def callback(ch, method, properties, body):
    print(" [x] Received {}".format(body))
    data = json.loads(body)
    time.sleep(2)
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def end_calc(id):
    rd = connect_redis()
    count = rd.get(id + "_count", 0)
    rd.set(id + "_count", count - 1)



def connect_redis():
    return redis.StrictRedis(host='localhost', port=6379, db=0)


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
    rd = connect_redis()
    rd.set(id + "row_", count - 1)

    for i in range(len(res)):
        row_delta = (i + offset_a) * row_size + offset_b
        for j in range(len(res[0])):
            if res[i][j] and row_delta + j < len(c):
                c[row_delta + j] += res[i][j]



channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='task_queue')

channel.start_consuming()
