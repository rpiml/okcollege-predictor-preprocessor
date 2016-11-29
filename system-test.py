import helpers
import json

'''
This short test script is used for verifying that the service integrates
with the entire system
'''

if __name__ == '__main__':

    rpc_client = helpers.RpcClient('predictor-preprocessor')

    with open('test_assets/example.json', 'r') as f:
        contents = f.read()

    result = rpc_client.call(contents)
    result_dict = json.loads(result)
    assert(result_dict["colleges"][0]["ranking"] == 1)
    assert(result_dict["colleges"][0]["name"] == "Rensselaer Polytechnic Institute")
