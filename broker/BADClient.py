#!/usr/bin/env python3

import requests

import simplejson as json
import time
import threading
import pika
import sys
import random
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
log = logging.getLogger(__name__)

class BADClient:
    def __init__(self, brokerServer, brokerPort=8989):
        self.dataverseName = None
        self.userName = None
        self.email = None
        self.password = None

        self.userId = None
        self.accessToken = None
        self.brokerPort = brokerPort
        self.brokerServer = brokerServer
        self.rabbitMQServer = brokerServer
        self.brokerUrl = 'http://{}:{}'.format(self.brokerServer, self.brokerPort)
        self.subscriptions = {}

        self.rqchannel = None

        self.on_channelresults = None
        self.on_error = self._on_error

    def runRabbitMQ(self, userId, callback, host):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.rqchannel = connection.channel()
        self.rqchannel.queue_declare(queue=userId)

        self.rqchannel.basic_consume(callback,
                              queue=userId,
                              no_ack=True)

        log.info('[*] Waiting for messages. To exit press CTRL+C')
        self.rqchannel.start_consuming()

    def register(self, dataverseName, userName, password, email=None):
        log.info('Register')

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
                if response['status'] == 'success':
                    self.userId = response['userId']
                    log.info('User `%s` registered.' % self.userName)
                else:
                    log.error('Registration failed for user `%s`' %self.userName)
                    self.on_error('register', 'Registration failed %s' %response)
        else:
            log.debug(r)
            self.on_error('register', 'Registration failed for %s' %userName)

    def login(self):
        log.info('Login')
        post_data = {'dataverseName': self.dataverseName, 'userName': self.userName, 'password': self.password}

        r = requests.post(self.brokerUrl + '/login', data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            if response:
                if response['status'] == 'success':
                    self.userId = response['userId']
                    self.accessToken = response['accessToken']
                    log.info('Login successful')
                    return True
                else:
                    log.error('Login failed %s' %response)
                    self.on_error('login', 'Login failed %s' %response)
                    return False
        else:
            log.debug(r)
            self.on_error('login', 'Login failed for %s' %self.userName)
            return False

    def logout(self):
        log.info('Logging out')

        if self.userId is None:
            return

        post_data = {
            'dataverseName': self.dataverseName, 'userId': self.userId, 'accessToken': self.accessToken
        }

        r = requests.post(self.brokerUrl + '/logout', data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            if response:
                if response['status'] == 'success':
                    log.info('User %s is logged out.' %self.userName)
                else:
                    log.debug(r)
                    self.on_error('logout', 'logout for usre %s failed due to %s' %(self.userName, r))
        else:
            log.debug(r)
            self.on_error('logout', 'Login failed for %s' %self.userName)
            return False

    def listchannels(self):
        post_data = {'dataverseName': self.dataverseName, 'userId': self.userId, 'accessToken': self.accessToken}

        r = requests.post(self.brokerUrl + "/listchannels", data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            log.debug(response)
        else:
            log.debug(r)
            self.on_error('listchannels', 'listchannels failed, call returned %s' % r)

    def listsubscriptions(self):
        post_data = {'dataverseName': self.dataverseName, 'userId': self.userId, 'accessToken': self.accessToken}

        r = requests.post(self.brokerUrl + "/listsubscriptions", data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            log.debug(response)
        else:
            log.debug(r)
            self.on_error('listsubscriptions', 'listsubscriptions failed, call returned %s' % r)

    def subscribe(self, channelName, parameters):
        log.info('Subscribe')

        if (channelName is None or parameters is None):
            log.info('Subscription failed: Empty channelname or callback')
            return

        if not self.on_channelresults:
            log.info('Subscription failed as no channel results callback is set')
            self.on_error('Subscription failed as no channel results callback is set')
            return None

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
                    log.info('Error:' +  response['error'])
                    return False
                else:
                    subscriptionId = response['userSubscriptionId']
                    timestamp = response['timestamp']

                    if subscriptionId not in self.subscriptions:
                        self.subscriptions[subscriptionId] = {}

                    self.subscriptions[subscriptionId]['channelName'] = channelName
                    self.subscriptions[subscriptionId]['parameters'] = parameters
                    self.subscriptions[subscriptionId]['timestamp'] = timestamp

                    log.info('Subscription successful')
                    return subscriptionId
        else:
            log.debug(r)
            self.on_error('subscribe', 'Subscription failed for channel %s with params %s' % (channelName, parameters))
            return None

    def _onNotifiedFromBroker(self, channel, method, properties, body):
        log.info('Notified from the broker')
        log.debug(str(body, encoding='utf-8'))

        response = json.loads(body)
        channelName = response['channelName']
        userSubscriptionId = response['userSubscriptionId']
        latestChannelExecutionTime = response['channelExecutionTime']

        self._getresults(channelName, userSubscriptionId, latestChannelExecutionTime)

    def insertrecords(self, datasetName, records):
        log.info('Insert records into %s' %datasetName)

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
                    log.info('Insert successful')
                else:
                    log.debug(r)
                    self.on_error('insertrecords', 'Error:', response['error'])

    def feedrecords(self, portNo, records):
        log.info('Feed records into port %s' %portNo)

        post_data = {'dataverseName': self.dataverseName,
                     'userId': self.userId,
                     'accessToken': self.accessToken,
                     'portNo': portNo,
                     'records': records
                     }

        r = requests.post(self.brokerUrl + '/feedrecords', data=json.dumps(post_data))

        if r.status_code == 200:
            response = r.json()
            if response:
                if response['status'] == 'success':
                    log.info('Insert successful')
                else:
                    log.debug(r)
                    self.on_error('feedrecords', 'Error:', response['error'])

    def _getresults(self, channelName, subscriptionId, channelExecutionTime):
        log.info('Getresults for %s' % subscriptionId)

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
                log.info('Retrived resultset for %s' %results['channelExecutionTimeReturned'])
                self.on_channelresults(channelName, subscriptionId, results['results'])

                resultsetsRemaining = results['resultsetsRemaining']
                log.info('%d resultsets are remaining' %resultsetsRemaining)
                if resultsetsRemaining > 0:
                    self._getresults(channelName, subscriptionId, channelExecutionTime)
            else:
                log.debug(r.text)
        else:
            log.debug(r.text)

    def _on_error(self, where, error_msg):
        log.error(where, ' --> ', error_msg)

    def run(self):
        self.runRabbitMQ(self.userId, callback=self._onNotifiedFromBroker, host=self.rabbitMQServer)

    def stop(self):
        log.info('Exiting client, username %s...' % self.userName)
        self.logout()
        if self.rqchannel and self.rqchannel.is_open:
            self.rqchannel.stop_consuming()

def test_client():
    def on_channelresults(channelName, subscriptionId, results):
        print(channelName, subscriptionId)
        if results and len(results) > 0:
            for item in results:
                print('APPDATA ' + str(item))

    def on_error(where, error_msg):
        print(where, ' ---> ', error_msg)

    client = BADClient(brokerServer='localhost')

    dataverseName = 'demoapp'
    userName = sys.argv[1]
    password = 'yusuf'
    email = 'abc@abc.net'

    client.on_channelresults = on_channelresults
    client.on_error = on_error
    
    client.register(dataverseName, userName, password, email)

    if client.login() == False:
        print('Login failed')
        return

    client.listchannels()
    client.listsubscriptions()

    #subcriptionId = client.subscribe('nearbyTweetChannel', ['man'])

    #client.subscribe('recentEmergenciesOfTypeChannel', ['tornado'], on_channelresults)
    #client.insertrecords('TweetMessageuuids', [{'message-text': 'Happy man'}, {'message-text': 'Sad man'}])

    # Feed created as per file 4
    '''
    data = [{'recordId': str(random.random()),
            'userId': '237',
            'userName': '343434',
            'password': '12245',
            'email': 'value@abc.net'
            },
            {'recordId': str(random.random()),
            'userId': '9999',
            'userName': '343434',
            'password': '12245',
            'email': 'value@abc.net'
            }
            ]

    client.feedrecords(10002, data)
    '''

    try:
        client.run() # blocking call
    except KeyboardInterrupt:
        client.stop()


if __name__ == "__main__":
    test_client()
