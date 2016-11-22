import pika
import redis
import csv
import io
import time
import json
import uuid

class RpcClient(object):
    def __init__(self, routing_key):

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
        if self.corr_id == props.correlation_id:
            self.response = body


    def call(self, body):
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

        return str(self.response)

    def __del__(self):
        self.connection.close()


def rabbitmq_connect(user='rabbitmq', password='rabbitmq', host='localhost'):
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
        return connection

def construct_type_dict(features_csv_bytes):
    features_csv = io.StringIO(features_csv_bytes.decode('utf-8'))
    reader = csv.reader(features_csv, delimiter='\t')

    type_dict = {}

    for row in reader:
        try:
            categorical_count = int(row[2])
        except:
            categorical_count = None
        type_dict[row[0]] = (row[1], categorical_count)

    return type_dict

def construct_feature_vector(survey_response, type_dict):
    response_vector = []
    seen_questions = set()
    try:
        for page in survey_response[1]['survey']['pages']:
            for question in page['questions']:
                if question['id'] not in type_dict:
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
                else:
                    response_vector.append((question['id'], None))
        unseen_questions = set(type_dict.keys()) - seen_questions
        for q in unseen_questions:
            response_vector.append((q, None))
    except LookupError:
        print('Malformed survey response provided: %s' % json.dumps(survey_response))
        raise

    return json.dumps([str(survey_response[0])] + [v[1] for v in sorted(response_vector, key=lambda x: x[0])])

def on_request(ch, method, props, body):
    r = redis.StrictRedis(host='localhost')
    print('Attempting to fetch survey_features.csv...')
    while True:
        features_csv_bytes = r.get('learning:survey_features.csv')
        if features_csv_bytes is not None:
            break
        else:
            print('survey_features.csv not found, trying again...')
            time.sleep(1)

    print('survey_features.csv fetched')

    try:
        print('Request received: %s' % str(survey_response[0]))
        print(body.decode('utf-8'))
        print(type(body))
        exit(1)
        survey_response = json.loads(body)
        type_dict = construct_type_dict(features_csv_bytes)
        feature_vector = construct_feature_vector(survey_response)

        rpc_client = RpcClient('predictor_queue')
        print('Requesting prediction from predictor...')
        response = rpc_client.call(feature_vector)
        print('Response received')
        del rpc_client

    except:
        print('Error: bad request %s' % str(body))
        response = None

    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id
        ),
        body=str(response)
    )
    ch.basic_ack(delivery_tag = method.delivery_tag)
