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
        self.write("This is BAD broker!!")


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

            platform = post_data['platform'] if 'platform' in post_data else None
            gcmRegistrationId = post_data['gcmRegistrationId'] if 'gcmRegistrationId' in post_data else None

            response = yield self.broker.register(dataverseName, userName, email, password, platform, gcmRegistrationId)

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
            gcmRegistrationId = None if 'gcmRegistrationId' not in post_data else post_data['gcmRegistrationId']

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
            subscriptionId = post_data['userSubscriptionId']
            deliveryTime = post_data['deliveryTime']

            response = yield self.broker.getresults(dataverseName, userId, accessToken, subscriptionId, deliveryTime)
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
        subscriptionIds = post_data['subscriptionIds']

        response = yield self.broker.notifyBroker(dataverseName, channelName, subscriptionIds)

        self.write(json.dumps(response))
        self.flush()
        self.finish()

def start_server():
    broker = BADBroker()

    application = tornado.web.Application([
        (r"/", MainHandler),
        (r"/register", RegistrationHandler, dict(broker=broker)),
        (r"/login", LoginHandler, dict(broker=broker)),
        (r"/logout", LogoutHandler, dict(broker=broker)),
        (r"/subscribe", SubscriptionHandler, dict(broker=broker)),
        (r"/unsubscribe", UnsubscriptionHandler, dict(broker=broker)),
        (r"/getresults", GetResultsHandler, dict(broker=broker)),
        (r"/notifybroker", NotifyBrokerHandler, dict(broker=broker))
    ])
    
    application.listen(8989)
    tornado.ioloop.IOLoop.current().start()
    
if __name__ == "__main__":
    start_server()
