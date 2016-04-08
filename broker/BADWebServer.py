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

        try:
            userName = post_data['userName']
            email = post_data['email']
            password = post_data['password']

            response = yield self.broker.register(userName, email, password)

        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request'}

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
            userName = post_data['userName']
            password = post_data['password']
            platform = 'desktop'

            response = yield self.broker.login(userName, password, platform)

        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request'}

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
            userId = post_data['userId']
            accessToken = post_data['accessToken']

            response = yield self.broker.logout(userId, accessToken)

        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request'}

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
            userId = post_data['userId']
            accessToken = post_data['accessToken']
            channelName = post_data['channelName']
            parameters = post_data['parameters']

            response = yield self.broker.subscribe(userId, accessToken, channelName, parameters)
        except KeyError as e:
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
            userId = post_data['userId']
            accessToken = post_data['accessToken']
            userSubscriptionId = post_data['userSubscriptionId']

            response = yield self.broker.unsubscribe(userId, accessToken, userSubscriptionId)
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
            userId = post_data['userId']
            accessToken = post_data['accessToken']
            channelName = post_data['channelName']
            subscriptionId = post_data['userSubscriptionId']
            deliveryTime = post_data['deliveryTime']

            response = yield self.broker.getresults(userId, accessToken, subscriptionId, deliveryTime)
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

        brokerName = None
        dataverseName = post_data['dataverseName']
        channelName = post_data['channelName']
        subscriptionIds = post_data['subscriptionIds']

        response = yield self.broker.notifyBroker(brokerName, dataverseName, channelName, subscriptionIds)

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
