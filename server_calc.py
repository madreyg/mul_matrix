from itertools import starmap

import pika
import time
import json
from operator import mul
import matrix

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


def connect_redis():
    return redis.StrictRedis(host='localhost', port=6379, db=0)


def write_matrix_sync(id, a, b, offset_a, offset_b, row_size):
    """метод выполняет основное вычислительное действие, поэтому его и распараллеливаем"""
    res = matrix.mul_atom(a, b)
    rd = connect_redis()
    l_name = id + "_result"
    for i in range(len(res)):
        row_delta = (i + offset_a) * row_size + offset_b
        for j in range(len(res[0])):
            if res[i][j] and row_delta + j < row_size:
                pipe = rd.pipeline()
                pipe.lset(l_name, row_delta + j, int(pipe.lindex(l_name, row_delta + j)) + res[i][j])
                pipe.execute()
    rd.decr(id + "count_")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='task_queue')

channel.start_consuming()
