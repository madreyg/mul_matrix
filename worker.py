import pika
import json
import matrix

import redis

print("Start worker")
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost', port=32774))
channel = connection.channel()
channel.queue_declare(queue='matrix_queue', durable=True)


def callback(ch, method, properties, body):
    # print(" [x] Received {}".format(body))
    # print(body.decode('utf8'))
    data = json.loads(body.decode('utf8'))
    matrix.recurse_mul(data["id"], data["left"], data["right"], int(data["n"]), int(data["offset_a"]),
                       int(data["offset_b"]),
                       int(data["max_size"]), int(data["size"]))
    # print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def connect_redis():
    return redis.StrictRedis(host='localhost', port=6379, db=0)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='task_queue')

channel.start_consuming()
