import tornado.ioloop
import tornado.web
import redis
import uuid
import matrix as mt
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
        self.write({'status': status, 'data': list(result)})
        self.finish()

    def worker_post(self):
        matrix = tornado.escape.json_decode(self.request.body)
        id_req = str(uuid.uuid4())
        rd = connect_redis()
        rd.set(id_req, '')
        left_mtx = matrix['a']
        right_mtx = matrix['b']
        self.write({'uuid': id_req})
        self.finish()
        res = run_mul(id_req, left_mtx, right_mtx)
        # result2 = mt.mul_on_numpy(a, b)
        rd.set(id_req, res)

    @tornado.web.asynchronous
    def get(self):
        self.worker(self.worker_get)

    @tornado.web.asynchronous
    def post(self):
        self.worker(self.worker_post)


def run_mul(id_req, a, b):
    res = mt.mul_matrix(id_req, a, b)
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
