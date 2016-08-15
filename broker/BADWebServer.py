#!/usr/bin/env python3

import tornado.ioloop
import tornado.web
import tornado.httpclient

import socket
import hashlib
import simplejson as json
import sys
from datetime import datetime
from BADBroker import BADBroker
from asterixapi import AsterixQueryManager

import logging as log

log.getLogger(__name__)
log.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=log.DEBUG)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('This is the BAD broker!!')


class RegistrationHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        dataverseName = 'channels'
        try:
            dataverseName = post_data['dataverseName']
            userName = post_data['userName']
            email = post_data['email']
            password = post_data['password']

            response = yield self.broker.register(dataverseName, userName, email, password)

        except KeyError as e:
            print('Parse error for ' + str(e) + ' in ' + str(post_data))
            print(e.with_traceback())
            response = {'status': 'failed', 'error': 'Bad formatted request ' + str(e)}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class LoginHandler (tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            dataverseName = post_data['dataverseName']
            userName = post_data['userName']
            password = post_data['password']
            platform = 'desktop' if 'platform' not in post_data else post_data['platform']
            gcmRegistrationId = '' if 'gcmRegistrationId' not in post_data else post_data['gcmRegistrationId']

            response = yield self.broker.login(dataverseName, userName, password, platform, gcmRegistrationId)

        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request ' + str(e)}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class LogoutHandler (tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            dataverseName = post_data['dataverseName']
            userId = post_data['userId']
            accessToken = post_data['accessToken']

            response = yield self.broker.logout(dataverseName, userId, accessToken)

        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request ' + str(e)}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class SubscriptionHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            dataverseName = post_data['dataverseName']
            userId = post_data['userId']
            accessToken = post_data['accessToken']
            channelName = post_data['channelName']
            parameters = post_data['parameters']

            response = yield self.broker.subscribe(dataverseName, userId, accessToken, channelName, parameters)
        except KeyError as e:
            log.error(str(e))
            response = {'status': 'failed', 'error': 'Bad formatted request'}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class UnsubscriptionHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            dataverseName = post_data['dataverseName']
            userId = post_data['userId']
            accessToken = post_data['accessToken']
            userSubscriptionId = post_data['userSubscriptionId']

            response = yield self.broker.unsubscribe(dataverseName, userId, accessToken, userSubscriptionId)
        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request'}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class GetResultsHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        print(self.request.body)

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            dataverseName = post_data['dataverseName']
            userId = post_data['userId']
            accessToken = post_data['accessToken']
            channelName = post_data['channelName']
            userSubscriptionId = post_data['userSubscriptionId']
            channelExecutionTime = post_data['channelExecutionTime']

            response = yield self.broker.getresults(dataverseName, userId, accessToken, userSubscriptionId, channelExecutionTime)
        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request'}

        print(json.dumps(response))
        self.write(json.dumps(response))
        self.flush()
        self.finish()


class NotifyBrokerHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        print(str(self.request.body, encoding='utf-8'))

    @tornado.gen.coroutine
    def post(self):
        log.info('Broker received notifybroker')
        log.info(str(self.request.body, encoding='utf-8'))

        post_data = json.loads(self.request.body)
        log.debug(post_data)

        dataverseName = post_data['dataverseName']
        channelName = post_data['channelName']
        channelExecutionTime = post_data['channelExecutionTime']
        subscriptionIds = post_data['subscriptionIds']

        response = yield self.broker.notifyBroker(dataverseName, channelName, channelExecutionTime, subscriptionIds)

        self.write(json.dumps(response))
        self.flush()
        self.finish()

class ListChannelsHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        print(str(self.request.body, encoding='utf-8'))

    @tornado.gen.coroutine
    def post(self):
        log.info('Broker received listchannels')
        log.info(str(self.request.body, encoding='utf-8'))

        post_data = json.loads(self.request.body)
        log.debug(post_data)

        dataverseName = post_data['dataverseName']
        userId = post_data['userId']
        accessToken = post_data['accessToken']

        response = yield self.broker.listchannels(dataverseName, userId, accessToken)

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class ListSubscriptionsHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        print(str(self.request.body, encoding='utf-8'))

    @tornado.gen.coroutine
    def post(self):
        log.info('Broker received listsubscriptions')
        log.info(str(self.request.body, encoding='utf-8'))

        post_data = json.loads(self.request.body)
        log.debug(post_data)

        dataverseName = post_data['dataverseName']
        userId = post_data['userId']
        accessToken = post_data['accessToken']

        response = yield self.broker.listsubscriptions(dataverseName, userId, accessToken)

        self.write(json.dumps(response, for_json=True))
        self.flush()
        self.finish()


def start_server():
    broker = BADBroker()

    application = tornado.web.Application([
        (r'/', MainHandler),
        (r'/register', RegistrationHandler, dict(broker=broker)),
        (r'/login', LoginHandler, dict(broker=broker)),
        (r'/logout', LogoutHandler, dict(broker=broker)),
        (r'/subscribe', SubscriptionHandler, dict(broker=broker)),
        (r'/unsubscribe', UnsubscriptionHandler, dict(broker=broker)),
        (r'/getresults', GetResultsHandler, dict(broker=broker)),
        (r'/notifybroker', NotifyBrokerHandler, dict(broker=broker)),
        (r'/listchannels', ListChannelsHandler, dict(broker=broker)),
        (r'/listsubscriptions', ListSubscriptionsHandler, dict(broker=broker))
    ])
    
    application.listen(8989)
    tornado.ioloop.IOLoop.current().start()
    
if __name__ == '__main__':
    start_server()
