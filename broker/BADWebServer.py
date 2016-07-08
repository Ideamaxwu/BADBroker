#!/usr/bin/env python3

import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.websocket
import os

import socket
import hashlib
import simplejson as json
import sys
from datetime import datetime
from BADBroker import BADBroker, add_websocket_listener
from asterixapi import AsterixQueryManager

import logging as log
from threading import Lock

log.getLogger(__name__)
log.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=log.DEBUG)
mutex = Lock()
condition_variable = False
live_web_sockets = set()


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        log.info("SAFIR")
        self.render("index.html")



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

            platform = 'desktop' if 'platform' not in post_data else post_data['platform']
            gcmRegistrationId = '' if 'gcmRegistrationId' not in post_data else post_data['gcmRegistrationId']

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
            gcmRegistrationId = '' if 'gcmRegistrationId' not in post_data else post_data['gcmRegistrationId']

            response = yield self.broker.login(dataverseName, userName, password, platform, gcmRegistrationId)

        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request ' + str(e)}

        log.debug(response)

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
        global condition_variable

        post_data = json.loads(self.request.body)
        log.debug(post_data)

        dataverseName = post_data['dataverseName']
        channelName = post_data['channelName']
        subscriptionIds = post_data['subscriptionIds']
        log.info('SAFIR' + str(subscriptionIds))

        response = yield self.broker.notifyBroker(dataverseName, channelName, subscriptionIds)

        mutex.acquire()
        try:
            condition_variable = True
        finally:
            mutex.release()

        self.write(json.dumps(response))
        self.flush()
        self.finish()

class NotificationsPageHandler(tornado.web.RequestHandler):
    def get(self):
        log.info("SAFIR entered notifications")
        self.render("notifications.html")

class PreferencePageHandler(tornado.web.RequestHandler):
    def get(self):
        log.info("SAFIR entered preferences")
        self.render("preferences.html")

class EventSourceHandler(tornado.web.RequestHandler):
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        log.info("SAFIR entered EventSourceHandler")
        global condition_variable
        send_fire_message = False
        mutex.acquire()
        try:
            if condition_variable == True:
                send_fire_message = True
                condition_variable = False
        finally:
            mutex.release()

        if send_fire_message == True:
            response = {'status': 'success', 'fire': 'true'}
        else:
            response = {'status': 'success', 'fire': 'false'}

        try:
            userId = post_data['userId']

            delivery_time = get_delivery_time(userId)

            if delivery_time >= 0:
                response['deliveryTime'] = delivery_time
            else:
                response = {'status': 'failed', 'error': 'Key Not found'}
        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request'}

        print(json.dumps(response))

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

class BrowserWebSocketHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        global live_web_sockets
        log.info("WebSocket opened")
        self.set_nodelay(True)
        mutex.acquire()
        try:
            live_web_sockets.add(self)
        finally:
            mutex.release()

    def on_message(self, message):
        print('Message incoming:', message)

    def on_close(self):
        log.info("WebSocket closed")

def get_web_sockets():
    global live_web_sockets
    web_sockets = None
    mutex.acquire()

    try:
        web_sockets = live_web_sockets
    finally:
        mutex.release()

    return web_sockets

def webSocketSendMessage(message):
    global live_web_sockets
    removable = set()
    mutex.acquire()
    try:
        for ws in live_web_sockets:
            if not ws.ws_connection or not ws.ws_connection.stream.socket:
                removable.add(ws)
            else:
                ws.write_message(message)
        for ws in removable:
            live_web_sockets.remove(ws)
    finally:
        mutex.release()

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

    settings = {
        "static_path": os.path.join(os.path.dirname(__file__), "static")
    }

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
        (r'/listsubscriptions', ListSubscriptionsHandler, dict(broker=broker)),
        (r"/notifications", NotificationsPageHandler),
        (r"/events", EventSourceHandler),
        (r"/preferences", PreferencePageHandler),
        (r"/websocketlistener", BrowserWebSocketHandler)
    ])

    application.listen(8989)
    tornado.ioloop.IOLoop.current().start()
    
if __name__ == '__main__':
    start_server()
