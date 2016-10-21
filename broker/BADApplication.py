import BADBroker
import requests
import simplejson as json

brokerUrl = "http://localhost:8989"

post_data = {'appName': 'demoapp', 'dataverseName': 'demoapp', 'email': 'demoapp@gmail.com'}

r = requests.post(brokerUrl + "/registerapplication", data=json.dumps(post_data))

print(r)

if r.status_code == 200:
    response = r.json()
    if response['status'] == 'success':
        print('Application registration successful')
        print('Got ApiKey ' + response['apiKey'])
    else:
        print('Application registration failed ' + response['error'])
