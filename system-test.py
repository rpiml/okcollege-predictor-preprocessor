import helpers

'''
This short test script is used for verifying that the service integrates
with the entire system
'''

if __name__ == '__main__':

    rpc_client = helpers.RpcClient('predictor-preprocessor')

    with open('test_assets/example.json', 'r') as f:
        contents = f.read()

    result = rpc_client.call(contents)
    print(result)
    assert(result == 'Rensselaer')
