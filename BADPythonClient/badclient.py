#!/usr/bin/env python

import requests
import json
import time
import threading
import pika
import sys
import random
import logging
import threading

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
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

        # Checking if broker is connected
        #self.checkBroker()

        log.info('[*] Waiting for messages. To exit press CTRL+C')
        self.rqchannel.start_consuming()

    def checkBroker(self):
        try:
            log.info('Checking broker')
            r = requests.get('%s:%d/heartbeat')
            if r and r.status_code == 204:
                log.info('Broker is alive')
                threading.Timer(10, self.checkBroker).start()

        except Exception as err:
            log.debug('Broker is not connected, quiting...')
            if self.rqchannel and self.rqchannel.is_open:
                self.rqchannel.stop_consuming()

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
                    return True
                else:
                    log.error('Registration failed for user `%s`' %self.userName)
                    self.on_error('register', 'Registration failed %s' %response)
        else:
            log.debug(r)
            self.on_error('register', 'Registration failed for %s' %userName)
        return False

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

        if self.userId is None or self.accessToken is None:
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
            print(response)
            subscriptions = []
            for item in response['subscriptions']:
                subscriptions.append(item['userSubscriptionId'])
            return subscriptions
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

        response = json.loads(str(body, encoding='utf-8'))
        channelName = response['channelName']
        userSubscriptionId = response['userSubscriptionId']
        latestChannelExecutionTime = response['channelExecutionTime']
        resultCount = response['resultCount']

        # Get all pending results
        results = self.getresults(userSubscriptionId)

        if results:
            success = self.on_channelresults(channelName, userSubscriptionId, results)
            if success:
                self.ackresults(channelName, userSubscriptionId, latestChannelExecutionTime)

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

    def getresults(self, subscriptionId, resultSize=None):
        log.info('Getresults for %s' % subscriptionId)

        post_data = {'dataverseName': self.dataverseName,
                     'userId': self.userId,
                     'accessToken': self.accessToken,
                     'userSubscriptionId': subscriptionId,
                     'resultSize': resultSize
                     }

        r = requests.post(self.brokerUrl + '/getresults', data=json.dumps(post_data))

        if r.status_code == 200:
            results = r.json()
            if results and results['status'] == 'success':
                channelExecutionTime = results['channelExecutionTime']
                log.info('Retrieved resultset for %s' %channelExecutionTime)
                return results['results']
            else:
                log.debug(r.text)
        else:
            log.debug(r.text)

    def getlatestresults(self, channelName, subscriptionId):
        log.info('Getresults for %s' % subscriptionId)

        post_data = {'dataverseName': self.dataverseName,
                     'userId': self.userId,
                     'accessToken': self.accessToken,
                     'channelName': channelName,
                     'userSubscriptionId': subscriptionId,
                     }

        r = requests.post(self.brokerUrl + '/getlatestresults', data=json.dumps(post_data))

        if r.status_code == 200:
            resultObject = r.json()
            if resultObject and resultObject['status'] == 'success':
                channelExecutionTime = resultObject['channelExecutionTime']
                log.info('Retrieved resultset for %s' %channelExecutionTime)
                return resultObject
            else:
                log.debug(r.text)
        else:
            log.debug(r.text)

    def ackresults(self, channelName, subscriptionId, channelExecutionTime):
        post_data = {'dataverseName': self.dataverseName,
                     'userId': self.userId,
                     'accessToken': self.accessToken,
                     'channelName': channelName,
                     'userSubscriptionId': subscriptionId,
                     'channelExecutionTime': channelExecutionTime
                     }
        log.info('ACK resultset for %s' %channelExecutionTime)
        r = requests.post(self.brokerUrl + '/ackresults', data=json.dumps(post_data))

        if r.status_code == 200:
            log.info('Results upto %s ACKed' %channelExecutionTime)
            return True
        else:
            log.debug(r.text)
            return False

    def callfunction(self, functionName, parameters):
        post_data = {'dataverseName': self.dataverseName,
                     'userId': self.userId,
                     'accessToken': self.accessToken,
                     'functionName': functionName,
                     'parameters': parameters
                     }

        log.info('Invoking function %s' %functionName)
        r = requests.post(self.brokerUrl + '/callfunction', data=json.dumps(post_data))

        if r.status_code == 200:
            log.info('Function called ' + r.text)
            return r.json()
        else:
            log.debug(r.text)
            return None

    def _on_error(self, where, error_msg):
        log.error(where, ' --> ', error_msg)

    def run(self):
        self.runRabbitMQ(self.userId, callback=self._onNotifiedFromBroker, host=self.rabbitMQServer)

    def stop(self):
        log.info('Exiting client, username %s...' %self.userName)
        self.logout()
        if self.rqchannel and self.rqchannel.is_open:
            self.rqchannel.stop_consuming()
