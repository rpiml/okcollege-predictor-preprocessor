'''
This script starts the predictor-preprocessor service, which receives
survey responses as json objects, preprocesses them into a json array,
sends them to the predictor service, and passes the result back up the chain
'''

import pika
import helpers
import colleges
import json
import logging

college_index = None

def on_request(prediction_response, respond):
    global college_index
    try:
        selected_colleges = [college_index[int(ind)] \
                                for ind in prediction_response.split(",")]
        response_dict = {'colleges': []}
        for i, college in enumerate(selected_colleges):
            response_dict['colleges'].append({
                "ranking": i+1,
                "name": selected_colleges[i]
            })

        respond(json.dumps(response_dict))
    except Exception as e:
        logging.exception("couldn't convert prediction response into college json")
        respond(json.dumps({}))



if __name__ == '__main__':

    print('Starting...')

    college_index = colleges.get_college_index()

    connection = helpers.rabbitmq_connect()
    channel = connection.channel()
    channel.queue_declare(queue='predictor-preprocessor')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(helpers.get_request(on_request), queue='predictor-preprocessor')

    helpers.wait_for_redis()

    print("Awaiting RPC requests...")
    channel.start_consuming()
