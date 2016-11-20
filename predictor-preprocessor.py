import pika
import redis
import csv
import io
import time
import json

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
        return None

    return json.dumps([str(survey_response[0])] + [v[1] for v in sorted(response_vector, key=lambda x: x[0])])


if __name__ == '__main__':
    r = redis.StrictRedis(host='localhost')

    while True:
        features_csv_bytes = r.get('learning:survey_features.csv')
        if features_csv_bytes is not None:
            break
        else:
            time.sleep(1)

    type_dict = construct_type_dict(features_csv_bytes)

    # features_csv = io.StringIO(features_csv_bytes.decode('utf-8'))
    #
    # reader = csv.reader(features_csv)
    #
    # for row in reader:
        # continue

    # print(features_csv.decode('utf-8'))
