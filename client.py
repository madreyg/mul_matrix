import sys
import json
import time
import requests
import random


def create_matr(n):
    a = [[0 for x in range(n)] for y in range(n)]
    for i in range(n):
        for j in range(n):
            a[i][j] = random.Random().randint(0, 9)
    return a


def check_status(uuid):
    print('run get')
    response = requests.get('http://localhost:8888/matrix/', {
        'uuid': uuid
    })
    response_json = response.json()
    return response_json


def main(n, m):
    response = requests.post('http://localhost:8888/matrix/', json.dumps({
        'a': create_matr(n),
        'b': create_matr(m),
    }))
    response_json = response.json()

    # TODO: uuid
    uuid = response_json.get('uuid')

    while True:
        result = check_status(uuid)
        status = result.get('status', False)
        print('status', status)
        if status:
            print('data', len(result.get('data', '')), "o my data shalala")
            break
        time.sleep(5)
    print('finished')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise Exception('incorect count of matrices')
    main(int(sys.argv[1]), int(sys.argv[2]))
