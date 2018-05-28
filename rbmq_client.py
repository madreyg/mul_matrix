import pika


def send_msg(msg):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost', port=32774))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    message = msg
    channel.basic_publish(exchange='',
                          routing_key='task_queue',
                          body=message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))
    print(" [x] Sent %r" % (message,))
    connection.close()
