import requests
import json
import sys

brokerUrl = "http://localhost:8989"
appName = sys.argv[1]
dataverseName = sys.argv[2]
setupAQL = ''

with open('app-%s.aql' %appName) as f:
    for line in f.readlines():
        if line and len(line) > 0:
            print(line)
            setupAQL += line

if setupAQL is None:
    print('The setup file app-%s.aql does not exist or reading from the failed' %appName)
    sys.exit(0)

post_data = {'appName': 'demoapp',
             'dataverseName': 'demoapp',
             'email': 'demoapp@gmail.com',
             }

r = requests.post(brokerUrl + "/registerapplication", data=json.dumps(post_data))

print(r.text)

if r.status_code == 200:
    response = r.json()
    if response['status'] == 'success':
        print('Application registration successful')
        print('Got ApiKey ' + response['apiKey'])
        apiKey = response['apiKey']

        post_data = {
            'appName': appName,
            'apiKey': apiKey,
            'setupAQL': setupAQL
        }
        r = requests.post(brokerUrl + "/setupapplication", data=json.dumps(post_data))
        print(r)

    else:
        print('Application registration failed ' + response['error'])
