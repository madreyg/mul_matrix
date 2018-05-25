import tornado.ioloop
import tornado.web
import uuid
import matrix
import threading


class Matrix(tornado.web.RequestHandler):
    def worker(self, func):
        t = threading.Thread(target=func)
        t.start()

    def worker_get(self):
        id_ = self.get_argument("uuid", default=None, strip=False)
        rd = matrix.connect_redis()
        status = int(rd.get(id_ + "_count"))
        print(int(status))
        answer = False
        result = []
        print(status == 0)
        print(type(status))
        if not status:
            answer = True
            result = rd.lpop(id_ + "_result")
        self.write({'status': answer, 'data': list(result)})
        self.finish()

    def worker_post(self):
        mtxs = tornado.escape.json_decode(self.request.body)
        id = str(uuid.uuid4())
        print("uuid: ", id)
        a = mtxs['a']
        b = mtxs['b']
        matrix.mul_matrix_sync(id, a, b)
        self.write({'uuid': id})
        self.finish()

    @tornado.web.asynchronous
    def get(self):
        self.worker(self.worker_get)

    @tornado.web.asynchronous
    def post(self):
        self.worker(self.worker_post)


def make_app():
    return tornado.web.Application([
        (r"/matrix/", Matrix),
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    print("Server started on localhost: 8888")
    tornado.ioloop.IOLoop.current().start()
