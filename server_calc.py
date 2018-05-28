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
    print(" [x] Received {}".format(body))
    print(body.decode('utf8'))
    data = json.loads(body.decode('utf8'))
    recurse_mul_sync(data["id"], data["left"], data["right"], int(data["n"]), int(data["offset_a"]),
                     int(data["offset_b"]),
                     int(data["max_size"]), int(data["size"]))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def connect_redis():
    return redis.StrictRedis(host='localhost', port=6379, db=0)


def write_matrix_sync(id, a, b, offset_a, offset_b, row_size, size):
    """метод выполняет основное вычислительное действие, поэтому его и распараллеливаем"""

    def client_side_incr(pipe):
        print("delta", row_delta + j)
        current_value = pipe.lindex(l_name, row_delta + j)
        next_value = int(current_value) + res[i][j]
        print("next_value", next_value)
        pipe.multi()
        pipe.lset(l_name, row_delta + j, next_value)

    print("run write_matrix_sync ", a)
    print("run write_matrix_sync ", b)
    print("run write_matrix_sync ", offset_a)
    print("run write_matrix_sync ", offset_b)
    print("run write_matrix_sync ", row_size)
    print("run write_matrix_sync ", size)
    res = matrix.mul_atom(a, b)
    print("res ", res)
    rd = connect_redis()
    l_name = id + "_result"
    for i in range(len(res)):
        row_delta = round((i + offset_a) * (row_size + size*matrix.ATOM_SIZE_MATRIX) + offset_b)
        print("row_delta ", row_delta)
        for j in range(len(res[0])):
            if res[i][j]:
                rd.transaction(client_side_incr, l_name)
                # rd.transaction(client_side_incr)
    print(id + "_count", rd.get(id + "_count"))
    res = rd.decr(id + "_count")
    print(res)


def recurse_mul_sync(id, a, b, n, offset_a, offset_b, row_size, size):
    """рекурсивный метод, разбивающий матрицу на более мелкие"""
    print("run recurse_mul_sync ", n)
    if n <= matrix.ATOM_SIZE_MATRIX:
        write_matrix_sync(id, a, b, offset_a, offset_b, n, size)
    else:
        n_new = n // 2
        a_new_11 = matrix.slice_spec(a, [0, n_new], [0, n_new])
        b_new_11 = matrix.slice_spec(b, [0, n_new], [0, n_new])
        a_new_12 = matrix.slice_spec(a, [n_new, n], [0, n_new])
        b_new_12 = matrix.slice_spec(b, [n_new, n], [0, n_new])
        a_new_21 = matrix.slice_spec(a, [0, n_new], [n_new, n])
        b_new_21 = matrix.slice_spec(b, [0, n_new], [n_new, n])
        a_new_22 = matrix.slice_spec(a, [n_new, n], [n_new, n])
        b_new_22 = matrix.slice_spec(b, [n_new, n], [n_new, n])
        # c11 a11b11 + a12b21
        # recurse_mul_sync(id, a_new_11, b_new_11, n_new, offset_a, offset_b, row_size),
        # recurse_mul_sync(id, a_new_12, b_new_21, n_new, offset_a, offset_b, row_size),
        matrix.send_to_worker(id, a_new_11, b_new_11, n_new, offset_a, offset_b, row_size, size)
        matrix.send_to_worker(id, a_new_12, b_new_21, n_new, offset_a, offset_b, row_size, size),
        # c12 a11b12 + a12b22
        # recurse_mul_sync(id, a_new_12, b_new_22, n_new, offset_a, offset_b + n_new, row_size),
        # recurse_mul_sync(id, a_new_11, b_new_12, n_new, offset_a, offset_b + n_new, row_size),
        matrix.send_to_worker(id, a_new_11, b_new_12, n_new, offset_a, offset_b + n_new, row_size, size),
        matrix.send_to_worker(id, a_new_12, b_new_22, n_new, offset_a, offset_b + n_new, row_size, size),
        # c21 a21b11 + a22b21
        # recurse_mul_sync(id, a_new_21, b_new_11, n_new, n_new + offset_a, offset_b, row_size),
        # recurse_mul_sync(id, a_new_22, b_new_21, n_new, n_new + offset_a, offset_b, row_size),
        matrix.send_to_worker(id, a_new_21, b_new_11, n_new, n_new + offset_a, offset_b, row_size, size),
        matrix.send_to_worker(id, a_new_22, b_new_21, n_new, n_new + offset_a, offset_b, row_size, size),
        # c22 a21b12 + a22b22
        matrix.send_to_worker(id, a_new_21, b_new_12, n_new, n_new + offset_a, offset_b + n_new, row_size, size),
        matrix.send_to_worker(id, a_new_22, b_new_22, n_new, n_new + offset_a, offset_b + n_new, row_size, size)
        # recurse_mul_sync(id, a_new_21, b_new_12, n_new, n_new + offset_a, offset_b + n_new, row_size),
        # recurse_mul_sync(id, a_new_22, b_new_22, n_new, n_new + offset_a, offset_b + n_new, row_size)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='task_queue')

channel.start_consuming()
