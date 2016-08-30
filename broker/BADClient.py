#!/usr/bin/env python3

import requests
import simplejson as json
import time
import threading
import pika
import sys

#brokerUrl = "http://cert24.ics.uci.edu:8989"
brokerUrl = "http://localhost:8989"

class BADClient:
    def __init__(self, brokerUrl):
        self.dataverseName = None
        self.userName = None
        self.email = None
        self.password = None

        self.userId = ""
        self.accessToken = ""
        self.brokerUrl = brokerUrl
        self.onNewResultCallback = None

        self.rqthread = None
        self.rqchannel = None

    def runRabbitQM(self, userId, callback, host='localhost'):
        def rabbitRun(host, userId):
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.rqchannel = connection.channel()

            self.rqchannel.queue_declare(queue=userId)

            self.rqchannel.basic_consume(callback,
                                  queue=userId,
                                  no_ack=True)

            print('[*] Waiting for messages. To exit press CTRL+C')
            self.rqchannel.start_consuming()

        self.rqthread = threading.Thread(target=rabbitRun, args=(host, userId))
        self.rqthread.start()

    def register(self, dataverseName, userName, password, email=None):
        print('Register')

        self.dataverseName = dataverseName
        self.userName = userName
        self.email = email
        self.password = password

        post_data = {'dataverseName': self.dataverseName, 'userName': self.userName, 'email': self.email, 'password': self.password}
        #response = service_call(URL, "register", post_data)
        r = requests.post(self.brokerUrl + '/register', data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            if response:
                if response['status'] != 'success':
                    print('Error:', response['error'])
                else:
                    self.userId = response['userId']
                    print(self.userName, 'Register', json.dumps(response))
        else:
            print(r)
            print('Registration failed for %s' %userName)            

    def login(self):
        print('Login')
        post_data = {'dataverseName': self.dataverseName, 'userName' : self.userName, 'password': self.password}
        #response = service_call(URL, "login", post_data)
        r = requests.post(self.brokerUrl + '/login', data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            if response:
                if response['status'] != 'success':
                    print('Error:', response['error'])
                    return False
                else:
                    self.userId = response['userId']
                    self.accessToken = response['accessToken']
                    print(self.userName, 'Login', json.dumps(response))
                    return True
        else:
            print('Login failed for %s' %self.userName) 
            print(r)      
            return False

    def listchannels(self):
        post_data = {'dataverseName': self.dataverseName, 'userId': self.userId, 'accessToken': self.accessToken}

        r = requests.post(self.brokerUrl + "/listchannels", data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            print(response)
        else:
            print('listchannels failed, call returned %s' % r)


    def listsubscriptions(self):
        post_data = {'dataverseName': self.dataverseName, 'userId': self.userId, 'accessToken': self.accessToken}
        r = requests.post(self.brokerUrl + "/listsubscriptions", data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            print(response)
        else:
            print('listsubscriptions failed, call returned %s' % r)

    def subscribe(self, channelName, parameters):
        print('Subscribe')

        if (channelName is None or parameters is None):
            print('Subscription failed: Empty channelname or callback')
            return

        if parameters is None:
            parameters = []

        post_data = {'dataverseName': self.dataverseName,
                     'userId': self.userId,
                     'accessToken': self.accessToken,
                     'channelName': channelName,
                     'parameters': parameters}

        r = requests.post(self.brokerUrl + '/subscribe', data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            if response:
                if response['status'] != 'success':
                    print('Error:', response['error'])
                    return False
                else:
                    subscriptionId = response['userSubscriptionId']
                    timestamp = response['timestamp']

                    print(self.userName, 'Subscribe', json.dumps(response))
                    return True
        else:
            print('Subscription failed for channel %s with params %s' % (channelName, parameters))
            return False

    def onNotifiedFromBroker(self, channel, method, properties, body):
        print('Notified from broker', str(body, encoding='utf-8'))
        response = json.loads(body)
        channelName = response['channelName']
        userSubscriptionId = response['userSubscriptionId']
        latestChannelExecutionTime = response['channelExecutionTime']

        self.getresults(channelName, userSubscriptionId, latestChannelExecutionTime)

    def insertrecords(self, datasetName, records):
        print('Insert records into %s' %datasetName)

        post_data = {'dataverseName': self.dataverseName,
                     'userId': self.userId,
                     'accessToken': self.accessToken,
                     'datasetName': datasetName,
                     'records': records
                     }

        r = requests.post(self.brokerUrl + '/insertrecords', data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            if response:
                if response['status'] == 'success':
                    print('Insert successful')
                else:
                    print('Error:', response['error'])

    def getresults(self, channelName, subscriptionId, channelExecutionTime):
        print('Getresults for %s' % subscriptionId)

        post_data = {'dataverseName': self.dataverseName,
                     'userId': self.userId,
                     'accessToken': self.accessToken,
                     'channelName': channelName,
                     'userSubscriptionId': subscriptionId,
                     'channelExecutionTime': channelExecutionTime
                     }

        r = requests.post(self.brokerUrl + '/getresults', data=json.dumps(post_data))

        if r.status_code == 200:
            results = r.json()
            if results and results['status'] == 'success':
                callback = self.onNewResultCallback
                callback(channelName, subscriptionId, results['channelExecutionTime'], results['results'])
            else:
                print('GetresultsError %s' % str(results['error']))

    def run(self):
        self.runRabbitQM(self.userId, self.onNotifiedFromBroker, host='localhost')

        if self.rqthread:
            print('Waiting for the messaging thread to stop...')
            try:
                while self.rqthread.isAlive():
                    self.rqthread.join(5)
            except KeyboardInterrupt as error:
                print('Closing rabbitMQ channel')
                self.rqchannel.stop_consuming()
                del self.rqchannel
                sys.exit(0)

    def __del__(self):
        print('Exiting client, username %s...' % self.userName)
        if self.rqchannel and self.rqchannel.is_open:
            self.rqchannel.cancel()


def test_client():
    def on_result(channelName, subscriptionId, channelExecutionTime, results):
        print(channelName, subscriptionId, channelExecutionTime)
        if results and len(results) > 0:
            for item in results:
                print('APPDATA ' + str(item))

    client = BADClient(brokerUrl=brokerUrl)

    dataverseName = sys.argv[1]
    userName = sys.argv[2]

    client.register(dataverseName, userName, 'yusuf', 'ddds@dsd.net')
    client.onNewResultCallback = on_result

    if client.login():
        client.listsubscriptions()
        #client.subscribe('recentEmergenciesOfTypeChannel', ['tornado'], on_result)
        client.subscribe('nearbyTweetChannel', ['Happy'])

        #client.listchannels()
        client.insertrecords('TweetMessageuuids', [{'message-text': 'Happy man'}, {'message-text': 'Sad man'}])

        client.run()
    else:
        print('Login failed')

if __name__ == "__main__":
    test_client()
