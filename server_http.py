import tornado.ioloop
import tornado.web
import redis
import uuid
import matrix as mt
from time import time
import threading
import pika
import sys
import json


class Matrix(tornado.web.RequestHandler):
    def worker(self, func):
        t = threading.Thread(target=func)
        t.start()

    def worker_get(self):
        id_ = self.get_argument("uuid", default=None, strip=False)
        rd = connect_redis()
        result = rd.get(id_)
        status = False
        if result:
            status = True
        # print(str(result))
        self.write({'status': status, 'data': list(result)})
        self.finish()

    def worker_post(self):
        matrix = tornado.escape.json_decode(self.request.body)
        id_ = str(uuid.uuid4())
        print("uuid: ", id_)
        rd = connect_redis()
        rd.set(id_, '')
        # a = matrix['a']
        # b = matrix['b']
        self.write({'uuid': id_})
        self.finish()
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost', port=32770))
        channel = connection.channel()

        channel.queue_declare(queue='task_queue', durable=True)

        message = json.dumps("id_:{}".format(matrix["a"]))
        channel.basic_publish(exchange='',
                              routing_key='task_queue',
                              body=message,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
        print(" [x] Sent %r" % (message,))
        connection.close()

    @tornado.web.asynchronous
    def get(self):
        self.worker(self.worker_get)

    @tornado.web.asynchronous
    def post(self):
        self.worker(self.worker_post)


def run_mul(a, b):
    res = mt.mul_matrix(a, b)
    return res


def run_mul_sync(a, b):
    res = mt.mul_matrix_sync(a, b)
    return res


def make_app():
    return tornado.web.Application([
        (r"/matrix/", Matrix),
    ])


def connect_redis():
    return redis.StrictRedis(host='localhost', port=6379, db=0)


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    print("Server started on localhost: 8888")
    tornado.ioloop.IOLoop.current().start()
