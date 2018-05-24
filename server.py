import tornado.ioloop
import tornado.web
import redis
import uuid
import matrix as mt
from time import time
import threading

a = 0

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
        a = matrix['a']
        b = matrix['b']
        self.write({'uuid': id_})
        self.finish()
        t1 = time()
        res = run_mul_sync(a, b)
        t2 = time()
        print("matrix calculated sync by uuid {id}. Time: {time}".format(id=id_, time=str(t2 - t1)))
        t1 = time()
        # res2 = run_mul_sync(a, b)
        t2 = time()
        print("matrix calculated by uuid {id}. Time: {time}".format(id=id_, time=str(t2 - t1)))
        print(res)
        t1 = time()
        result2 = mt.mul_on_numpy(a, b)
        t2 = time()
        print("Проверка: " + str(list(result2)) + " time: " + str(t2-t1))
        # print("Проверка: "  + " time: " + str(t2-t1))


        rd.set(id_, res)

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
