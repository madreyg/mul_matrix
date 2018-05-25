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
                     int(data["max_size"]))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def connect_redis():
    return redis.StrictRedis(host='localhost', port=6379, db=0)


def write_matrix_sync(id, a, b, offset_a, offset_b, row_size):
    """метод выполняет основное вычислительное действие, поэтому его и распараллеливаем"""
    res = matrix.mul_atom(a, b)
    rd = connect_redis()
    l_name = id + "_result"
    # return 0
    for i in range(len(res)):
        row_delta = (i + offset_a) * row_size + offset_b
        for j in range(len(res[0])):
            if res[i][j] and row_delta + j < row_size:
                def client_side_incr(pipe):
                    current_value = pipe.lindex(l_name, -(row_delta + j))
                    next_value = int(current_value) + + res[i][j]
                    print(next_value)
                    pipe.multi()
                    pipe.lset(l_name, -(row_delta + j), next_value)

                rd.transaction(client_side_incr, l_name)
                # pipe = rd.pipeline()
                # print(-(row_delta + j))
                # tmp = pipe.lindex(l_name, -(row_delta + j))
                # print(tmp)
                # print(l_name)
                # pipe.lset(l_name, -(row_delta + j), int(tmp) + res[i][j])
                # pipe.execute()
    # return 0
    print(rd.get(id + "_count"))
    res = rd.decr(id + "_count")
    print(res)


def recurse_mul_sync(id, a, b, n, offset_a, offset_b, row_size):
    """рекурсивный метод, разбивающий матрицу на более мелкие"""
    if n <= matrix.ATOM_SIZE_MATRIX:
        write_matrix_sync(id, a, b, offset_a, offset_b, row_size)
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
        matrix.send_rbmq(id, a_new_11, b_new_11, n_new, offset_a, offset_b, row_size)
        matrix.send_rbmq(id, a_new_12, b_new_21, n_new, offset_a, offset_b, row_size),
        # recurse_mul_sync(id, a_new_12, b_new_21, n_new, offset_a, offset_b, row_size),
        # c12 a11b12 + a12b22
        # recurse_mul_sync(id, a_new_11, b_new_12, n_new, offset_a, offset_b + n_new, row_size),
        matrix.send_rbmq(id, a_new_11, b_new_12, n_new, offset_a, offset_b + n_new, row_size),
        matrix.send_rbmq(id, a_new_12, b_new_22, n_new, offset_a, offset_b + n_new, row_size),
        # recurse_mul_sync(id, a_new_12, b_new_22, n_new, offset_a, offset_b + n_new, row_size),
        # c21 a21b11 + a22b21
        matrix.send_rbmq(id, a_new_21, b_new_11, n_new, n_new + offset_a, offset_b, row_size),
        # recurse_mul_sync(id, a_new_21, b_new_11, n_new, n_new + offset_a, offset_b, row_size),
        # recurse_mul_sync(id, a_new_22, b_new_21, n_new, n_new + offset_a, offset_b, row_size),
        matrix.send_rbmq(id, a_new_22, b_new_21, n_new, n_new + offset_a, offset_b, row_size),
        # c22 a21b12 + a22b22
        matrix.send_rbmq(id, a_new_21, b_new_12, n_new, n_new + offset_a, offset_b + n_new, row_size),
        # recurse_mul_sync(id, a_new_21, b_new_12, n_new, n_new + offset_a, offset_b + n_new, row_size),
        # recurse_mul_sync(id, a_new_22, b_new_22, n_new, n_new + offset_a, offset_b + n_new, row_size)
        matrix.send_rbmq(id, a_new_22, b_new_22, n_new, n_new + offset_a, offset_b + n_new, row_size)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='task_queue')

channel.start_consuming()
