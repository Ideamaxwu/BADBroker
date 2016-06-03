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
        self.subscriptions = {}
        self.brokerUrl = brokerUrl

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
        post_data = {'userName' : self.userName, 'password': self.password}
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

    def subscribe(self, channelName, parameters, callback):
        print('Subscribe')

        if (channelName is None or parameters is None):
            print('Subscription failed: Empty channelname or callback')
            return

        if parameters is None:
            parameters = []

        post_data = {'userId' : self.userId, 'accessToken': self.accessToken,
            'channelName': channelName, 'parameters': parameters}

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

                    if channelName is not self.subscriptions:
                        self.subscriptions[channelName] = {}

                    self.subscriptions[channelName][subscriptionId] = {
                        'subscriptionId': subscriptionId,
                        'deliveryTime': timestamp,
                        'channelName': channelName,
                        'parameters': parameters,
                        'callback': callback
                    }

                    print(self.userName, 'Subscribe', json.dumps(response))
                    return True
        else:
            print('Subscription failed for channel %s with params' % (channelName, parameters))
            return False

    def onNotifiedFromBroker(self, channel, method, properties, body):
        print('Notified from broker', str(body, encoding='utf-8'))
        response = json.loads(body)
        channelName = response['channelName']
        lastestDeliveryTime = response['timestamp']

        print(self.subscriptions)

        if channelName not in self.subscriptions:
            print('No active subscription for channel', channelName)
        else:
            for subscriptionId in self.subscriptions[channelName]:
                self.getresults(channelName, subscriptionId, lastestDeliveryTime)

    def getresults(self, channelName, subscriptionId, deliveryTime):
        print('Getresults for %s' % subscriptionId)

        post_data = {'userId': self.userId,
                     'accessToken': self.accessToken,
                     'channelName': channelName,
                     'userSubscriptionId': subscriptionId,
                     'deliveryTime': deliveryTime
                     }

        r = requests.post(self.brokerUrl + '/getresults', data=json.dumps(post_data))

        if r.status_code == 200:
            results = r.json()
            if results and results['status'] == 'success':
                callback = self.subscriptions[channelName][subscriptionId]['callback']
                callback(channelName, subscriptionId, results['deliveryTime'], results['results'])

                # update deliveryTime
                for key in self.subscriptions[channelName]:
                    self.subscriptions[channelName][key]['deliveryTime'] = deliveryTime
            else:
                print('GetresultsError %s' % str(results['error']))

    def onNewResultsOnChannel(self, channelName, subscriptionId, deliveryTime, results):
        print(self.userName, channelName, 'Channel results', results)

        if channelName not in self.subscriptions or subscriptionId not in self.subscriptions[channelName]:
            print('No active subscription for this channel')
            return

        if results:
            results = results.replace('\n', '')
            allResultItems = json.loads(results)['results']

            print('More new results received', len(allResultItems))
            for item in allResultItems:
                print(item)

    def setCommands(self):
        with open('client-commands.txt') as f:
            for line in f.readlines():
                if line.startswith("#") or len(line) <= 1:
                    continue
                print(line)
                command = json.loads(line)
                if 'delay' in command:
                    delay = command['delay']
                    time.sleep(delay)

                if command['command'] == 'register':
                    if len(sys.argv) >= 4:
                        self.userName = sys.argv[1]
                        self.password = sys.argv[2]
                        self.email = sys.argv[3]
                        self.register(self.userName, self.password, self.email)
                    else:
                        print('Usage python3 BADClient userName email password')
                        sys.exit(0)

                elif command['command'] == 'login':
                    self.login(self.userName, self.password)

                elif command['command'] == 'subscribe':
                    self.subscribe(command['channelName'], command['parameters'])

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
    def on_result(channelName, subscriptionId, deliveryTime, results):
        print(channelName, subscriptionId, deliveryTime)
        for item in results:
            print(item)

    client = BADClient(brokerUrl=brokerUrl)
    client.register(sys.argv[1], sys.argv[2], 'yusuf', 'abc@net.com')

    sys.exit(0)

    if client.login():
        if client.subscribe('nearbyTweetChannel', ['man'], on_result):
        #if client.subscribe('recentEmergenciesOfTypeChannel', ['earthquake'], on_result):
            # Blocking call
            client.run()
    else:
        print('Login failed')

if __name__ == "__main__":
    test_client()
