import sys
import json
import time
import requests
import matrix


def check_status(id_req):
    """
    метод для отправки get запроса для проверки статуса вычислений
    """
    response = requests.get('http://localhost:8888/matrix/', {
        'uuid': id_req
    })
    response_json = response.json()
    return response_json


def run_mul(n: int, m: int):
    """
    метод для отправки post запроса для запуска вычисления
    """
    mtx_a = matrix.create_mtx(n)
    mtx_b = matrix.create_mtx(m)
    response = requests.post('http://localhost:8888/matrix/', json.dumps({
        'a': mtx_a,
        'b': mtx_b,
    }))
    response_json = response.json()

    # TODO: uuid
    uuid = response_json.get('uuid')

    while True:
        result = check_status(uuid)
        status = result.get('status', False)
        print('status', status)
        if status:
            print('data', result.get('data', ''), "o my data shalala")
            # todo: выпилить, как заончить отладочную настройку и модификацию алгоритма перемножения
            # print("проверка ", matrix.mul_on_numpy(mtx_a, mtx_b))
            break
        time.sleep(5)
    print('finished')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise Exception('incorect count of matrices')
    run_mul(int(sys.argv[1]), int(sys.argv[2]))
