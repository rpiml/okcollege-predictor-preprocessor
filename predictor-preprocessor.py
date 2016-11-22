import pika
import helpers

'''
This script starts the predictor-preprocessor service, which receives
survey responses as json objects, preprocesses them into a json array,
sends them to the predictor service, and passes the result back up the chain
'''

if __name__ == '__main__':

    print('Starting...')
    connection = helpers.rabbitmq_connect()
    channel = connection.channel()
    channel.queue_declare(queue='predictor-preprocessor')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(helpers.on_request, queue='predictor-preprocessor')

    print("Awaiting RPC requests...")
    channel.start_consuming()
