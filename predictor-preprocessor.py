import pika
import redis
import csv
import io
import time
import json
import uuid
import helpers

if __name__ == '__main__':

    connection = helpers.rabbitmq_connect()
    channel = connection.channel()
    channel.queue_declare(queue='predictor-preprocessor')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(helpers.on_request, queue='predictor-preprocessor')

    print("Awaiting RPC requests")
    channel.start_consuming()
