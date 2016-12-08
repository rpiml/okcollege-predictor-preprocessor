import pika
import redis
import csv
import io
import time
import json
import uuid
import os

'''
Collection of helper functions / classes for use in the predictor-preprocessor
'''

class RpcClient(object):
    '''
    A class to instantiate a client for making remote procedure calls with RabbitMQ
    '''
    def __init__(self, routing_key):
        '''
        Setup and configure the RabbitMQ connection for RPC
        '''
        self.connection = rabbitmq_connect()
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.routing_key = routing_key

        self.channel.basic_consume(
            self._on_response,
            no_ack=True,
            queue=self.callback_queue
        )

    def _on_response(self, ch, method, props, body):
        '''
        Callback to check if the received response belongs to the current request
        '''
        if self.corr_id == props.correlation_id:
            self.response = body


    def call(self, body):
        '''
        Method to queue a request to the specified routing_key and wait for
        response to be populated
        '''
        self.response = None
        self.corr_id = str(uuid.uuid4())

        properties = pika.BasicProperties(
            reply_to = self.callback_queue,
            correlation_id = self.corr_id
        )

        self.channel.basic_publish(
            exchange='',
            routing_key=self.routing_key,
            properties=properties,
            body=body
        )

        while self.response is None:
            self.connection.process_data_events()

        return self.response.decode('utf-8')

    def __del__(self):
        self.connection.close()

def rabbitmq_connect(user='rabbitmq', password='rabbitmq', host=None):
    '''
    Connect to a rabbitmq server and wait for the connection to be established
    '''
    if host is None:
        host = os.getenv('RABBITMQ_HOST') or 'localhost'
    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(
        host=host,
        credentials=credentials
    )
    print('Attempting RabbitMQ connection...')
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            break
        except Exception as e:
            print('Could not connect to RabbitMQ. Retrying...')
            time.sleep(1)

    print('RabbitMQ connection established')
    return connection

def wait_for_redis(host=None):
    '''
    Block until the redis service is running
    '''

    if host is None:
        host = (os.getenv('REDIS_HOST') or 'localhost')

    print('Confirming redis service is running...')
    r = redis.StrictRedis(host=(os.getenv('REDIS_HOST') or 'localhost'))
    while True:
        try:
            r.ping()
            break
        except (redis.exceptions.ConnectionError, redis.exceptions.BusyLoadingError):
            print('Could not connect to redis. Retrying...')
            time.sleep(1)

    print('Redis service confirmed running.')

def construct_feature_dict(features_csv_bytes):
    '''
    This function takes:
        - a csv file (as a byte array) where rows are of the format:
                    question_id     question_type       categorical_count (if any)

    and returns:
        - a dictionary of format {question_id : ('numerical' || 'categorical', categorical_count)}

    '''
    features_csv = io.StringIO(features_csv_bytes.decode('utf-8'))
    reader = csv.reader(features_csv, delimiter='\t')

    feature_dict = {}

    for row in reader:
        try:
            categorical_count = int(row[2])
        except:
            categorical_count = None
        feature_dict[row[0]] = (row[1], categorical_count)

    return feature_dict

def construct_feature_vector(survey_response, feature_dict):
    '''
    This function takes:
        - a survey response (as a json object)
        - a dictionary of format {question_id : ('numerical' || 'categorical', categorical_count)}

    and returns serialized json list where:
        - every entry corresponds to a feature
        Note:
            Multi-choice questions are broken up into a series of categorical
            questions of count 2 (i.e. booleans)

    '''
    response_vector = []
    seen_questions = set()
    try:
        for page in survey_response['survey']['pages']:
            for question in page['questions']:
                if question['id'] not in feature_dict:
                    continue

                seen_questions.add((question['id']))

                if 'answer' not in question:
                    response_vector.append((question['id'], None))
                    continue

                if question['type'] == 'slider':
                    response_vector.append((question['id'], question['answer']))

                elif question['type'] == 'choice' or question['type'] == 'multi-choice-dropdown':
                    i = question['answers'].index(question['answer'])
                    response_vector.append((question['id'], i))

                elif question['type'] == 'multi-choice':
                    answer = set(question['answer'])
                    for q in question['answers']:
                        name = question['id'] + ':' + q
                        if q in answer:
                            response_vector.append((name, 1.))
                        else:
                            response_vector.append((name, 0.))
                elif question['type'] == 'text':
                    response_vector.append((question['id'], question['answer']))
                else:
                    response_vector.append((question['id'], None))
        unseen_questions = set(feature_dict.keys()) - seen_questions
        for q in unseen_questions:
            response_vector.append((q, None))
    except LookupError:
        print('Malformed survey response provided: %s' % json.dumps(survey_response))
        raise

    return json.dumps([v[1] for v in sorted(response_vector, key=lambda x: x[0])])

def get_survey_features():
    r = redis.StrictRedis(host=(os.getenv('REDIS_HOST') or 'localhost'))
    print('Attempting to fetch survey_features.csv...')
    while True:
        features_csv_bytes = r.get('learning:survey_features.csv')
        if features_csv_bytes is not None:
            break
        else:
            print('survey_features.csv not found, trying again...')
            time.sleep(1)
    return construct_feature_dict(features_csv_bytes)

def get_request(response_callback):

    def on_request(ch, method, props, body):
        '''
        Callback to handle incoming requests to the predictor-preprocessor queue.
        Hands off preprocessed data to the predictor and returns the response back to
        the initial requester.
        '''
        try:
            survey_response = json.loads(body.decode('utf-8'))
            print('Request received, parsing...')

            feature_dict = get_survey_features()
            feature_vector = construct_feature_vector(survey_response, feature_dict)

            rpc_client = RpcClient('predictor_queue')
            print('Requesting prediction from predictor...')
            response = rpc_client.call(feature_vector)
            print('Response received')
            del rpc_client

        except:
            print('Error: bad request %s' % str(body))
            response = None

        def respond(content):
            publish_response(ch, method, props, content)

        response_callback(response, respond)

    return on_request

def publish_response(ch, method, props, response):
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id
        ),
        body=str(response)
    )
    ch.basic_ack(delivery_tag = method.delivery_tag)
