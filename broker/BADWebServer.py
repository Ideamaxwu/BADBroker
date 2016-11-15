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
from BADBroker import BADBroker, set_live_web_sockets
from asterixapi import AsterixQueryManager

import logging as log
from threading import Lock

import brokerutils

log = brokerutils.setup_logging(__name__)

mutex = Lock()
condition_variable = False
live_web_sockets = set()


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        log.info("MAIN")
        self.render("static/index.html")
        #self.render("htmlpages/registerapp.html")


class RegisterApplicationHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        response = None

        if len(self.get_body_arguments('fromForm')) > 0:
            try:
                appName = self.get_body_argument('appName')
                appDataverse = self.get_body_argument('appDataverse')
                adminUser = self.get_body_argument('adminUser')
                adminPasseword = self.get_body_argument('adminPassword')
                email = self.get_body_argument('email')
                dropExisting = self.get_body_arguments('dropExisting')
                setupAQL = self.get_body_arguments('setupAQL')

                if len(dropExisting) > 0 and len(setupAQL) > 0:
                    response = yield self.broker.registerApplication(appName, appDataverse, adminUser, adminPasseword, email, dropExisting=1, setupAQL=setupAQL[0])
                elif len(dropExisting) > 0:
                    response = yield self.broker.registerApplication(appName, appDataverse, adminUser, adminPasseword, email, dropExisting=1)
                elif len(setupAQL) > 0:
                    response = yield self.broker.registerApplication(appName, appDataverse, adminUser, adminPasseword, email, setupAQL=setupAQL[0])
                else:
                    response = yield self.broker.registerApplication(appName, appDataverse, adminUser, adminPasseword, email)

            except tornado.web.MissingArgumentError as e:
                log.error(e.with_traceback)
                response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}
            except Exception as e:
                log.error(response)
                response = {'status': 'failed', 'error': str(e)}
        else:
            post_data = json.loads(str(self.request.body, encoding='utf-8'))
            log.debug(post_data)
            try:
                appName = post_data['appName']
                appDataverse = post_data['appDataverse']
                adminUser = post_data['adminUser']
                adminPasseword = post_data['adminPassword']
                email = post_data['email']
                dropExisting = post_data['dropExisting'] if 'dropExisting' in post_data else 0
                setupAQL = post_data['setupAQL'] if 'setupAQL' in post_data else None

                response = yield self.broker.registerApplication(appName, appDataverse, adminUser, adminPasseword, email, dropExisting, setupAQL)

            except KeyError as e:
                log.info('Parse error for ' + str(e) + ' in ' + str(post_data))
                log.info(e.with_traceback())
                response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class SetupApplicationHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            appName = post_data['appName']
            apiKey = post_data['apiKey']
            setupAQL = post_data['setupAQL']

            response = yield self.broker.setupApplication(appName, apiKey, setupAQL)

        except KeyError as e:
            log.info('Parse error for ' + str(e) + ' in ' + str(post_data))
            log.info(e.with_traceback())
            response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class UpdateApplicationHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            appName = post_data['appName']
            apiKey = post_data['apiKey']
            setupAQL = post_data['setupAQL']

            response = yield self.broker.setupApplication(appName, apiKey, setupAQL)

        except KeyError as e:
            log.info('Parse error for ' + str(e) + ' in ' + str(post_data))
            log.info(e.with_traceback())
            response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class ApplicationAdminLoginHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            appName = post_data['appName']
            adminUser = post_data['adminUser']
            adminPassword = post_data['adminPassword']

            response = yield self.broker.applicationAdminLogin(appName, adminUser, adminPassword)

        except KeyError as e:
            log.info('Parse error for ' + str(e) + ' in ' + str(post_data))
            log.info(e.with_traceback())
            response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class AdminQueryHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            appName = post_data['appName']
            apiKey = post_data['apiKey']
            query = post_data['query']

            if query == 'listchannels':
                response = yield self.broker.adminQueryListChannels(appName, apiKey)
            elif query == 'listsubscriptions':
                channelName = post_data['channelName']
                response = yield self.broker.adminQueryListSubscriptions(appName, apiKey, channelName)
            else:
                response = {
                    'status': 'failed',
                    'error': 'Invalid query'
                }

        except KeyError as e:
            log.info('Parse error for ' + str(e) + ' in ' + str(post_data))
            log.info(e.with_traceback())
            response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class RegistrationHandler(tornado.web.RequestHandler):
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
            email = post_data['email']
            password = post_data['password']

            response = yield self.broker.register(dataverseName, userName, password, email)

        except KeyError as e:
            log.info('Parse error for ' + str(e) + ' in ' + str(post_data))
            log.info(e.with_traceback())
            response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}

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

            response = yield self.broker.login(dataverseName, userName, password, platform)

        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}

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
            response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}

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
        log.info(self.request.body)

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
            response = {'status': 'failed', 'error': 'Bad formatted request missing field ' + str(e)}

        log.info(json.dumps(response))
        self.write(json.dumps(response))
        self.flush()
        self.finish()


class AckResultsHandler(tornado.web.RequestHandler):
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

            response = yield self.broker.ackresults(dataverseName, userId, accessToken, userSubscriptionId, channelExecutionTime)
        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request ' + str(e)}

        print(json.dumps(response))
        self.write(json.dumps(response))
        self.flush()
        self.finish()


class InsertRecordsHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        log.info(self.request.body)

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            dataverseName = post_data['dataverseName']
            userId = post_data['userId']
            accessToken = post_data['accessToken']
            datasetName = post_data['datasetName']
            records = post_data['records']

            response = yield self.broker.insertrecords(dataverseName, userId, accessToken, datasetName, records)
        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request'}

        log.info(json.dumps(response))
        self.write(json.dumps(response))
        self.flush()
        self.finish()


class FeedRecordsHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        log.info(self.request.body)

    @tornado.gen.coroutine
    def post(self):
        log.info(str(self.request.body, encoding='utf-8'))
        post_data = json.loads(str(self.request.body, encoding='utf-8'))

        log.debug(post_data)

        try:
            dataverseName = post_data['dataverseName']
            userId = post_data['userId']
            accessToken = post_data['accessToken']
            portNo = post_data['portNo']
            records = post_data['records']

            response = yield self.broker.feedrecords(dataverseName, userId, accessToken, portNo, records)
        except KeyError as e:
            response = {'status': 'failed', 'error': 'Bad formatted request'}

        log.info(json.dumps(response))
        self.write(json.dumps(response))
        self.flush()
        self.finish()


class NotifyBrokerHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        log.info(str(self.request.body, encoding='utf-8'))

    @tornado.gen.coroutine
    def post(self):
        log.info('Broker received notifybroker')
        log.info(str(self.request.body, encoding='utf-8'))
        global condition_variable

        post_data = json.loads(self.request.body)
        log.debug(post_data)

        dataverseName = post_data['dataverseName']
        channelName = post_data['channelName']
        channelExecutionTime = post_data['channelExecutionTime']
        subscriptionIds = post_data['subscriptionIds']

        response = yield self.broker.notifyBroker(dataverseName, channelName, channelExecutionTime, subscriptionIds)

        mutex.acquire()
        try:
            condition_variable = True
        finally:
            mutex.release()

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class GCMRegistrationHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        log.info(str(self.request.body, encoding='utf-8'))

    def post(self):
        log.info('Broker received gcmregistration')
        log.info(str(self.request.body, encoding='utf-8'))

        post_data = json.loads(self.request.body)
        log.debug(post_data)

        dataverseName = post_data['dataverseName']
        userId = post_data['userId']
        accessToken = post_data['accessToken']
        gcmRegistrationToken = post_data['gcmRegistrationToken']

        response = self.broker.gcmRegistration(dataverseName, userId, accessToken, gcmRegistrationToken)

        self.write(json.dumps(response))
        self.flush()
        self.finish()


class NotificationsPageHandler(tornado.web.RequestHandler):
    def get(self):
        log.info("Entered notifications")
        self.render("notifications.html")


class PreferencePageHandler(tornado.web.RequestHandler):
    def get(self):
        log.info("Entered preferences")
        self.render("preferences.html")

class SubscriptionPageHandler(tornado.web.RequestHandler):
    def get(self):
        log.info("Entered subscriptions")
        self.render("subscriptions.html")


class LocationSubscriptionPageHandler(tornado.web.RequestHandler):
    def get(self):
        log.info("Entered location subscriptions")
        self.render("locationsubs.html")


class ListChannelsHandler(tornado.web.RequestHandler):
    def initialize(self, broker):
        self.broker = broker

    def get(self):
        log.info(str(self.request.body, encoding='utf-8'))

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
            set_live_web_sockets(self)
        finally:
            mutex.release()

    def on_message(self, message):
        log.info('Message incoming:', message)

    def on_close(self):
        log.info("WebSocket closed")

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
        log.info(str(self.request.body, encoding='utf-8'))

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
    broker = BADBroker.getInstance()

    settings = {
        "static_path": os.path.join(os.path.dirname(__file__), "static")
    }

    application = tornado.web.Application([
        (r'/', MainHandler),
        (r'/registerapplication', RegisterApplicationHandler, dict(broker=broker)),
        (r'/setupapplication', SetupApplicationHandler, dict(broker=broker)),
        (r'/updateapplication', UpdateApplicationHandler, dict(broker=broker)),
        (r'/appadminlogin', ApplicationAdminLoginHandler, dict(broker=broker)),
        (r'/adminquery', AdminQueryHandler, dict(broker=broker)),
        (r'/register', RegistrationHandler, dict(broker=broker)),
        (r'/login', LoginHandler, dict(broker=broker)),
        (r'/logout', LogoutHandler, dict(broker=broker)),
        (r'/subscribe', SubscriptionHandler, dict(broker=broker)),
        (r'/unsubscribe', UnsubscriptionHandler, dict(broker=broker)),
        (r'/getresults', GetResultsHandler, dict(broker=broker)),
        (r'/ackresults', AckResultsHandler, dict(broker=broker)),
        (r'/notifybroker', NotifyBrokerHandler, dict(broker=broker)),
        (r'/listchannels', ListChannelsHandler, dict(broker=broker)),
        (r'/listsubscriptions', ListSubscriptionsHandler, dict(broker=broker)),
        (r'/gcmregistration', GCMRegistrationHandler, dict(broker=broker)),
        (r'/notifications', NotificationsPageHandler),
        (r'/preferences', PreferencePageHandler),
        (r'/websocketlistener', BrowserWebSocketHandler),
        (r'/subscriptions', SubscriptionPageHandler),
        (r'/locationsubs', LocationSubscriptionPageHandler),
        (r'/insertrecords', InsertRecordsHandler, dict(broker=broker)),
        (r'/feedrecords', FeedRecordsHandler, dict(broker=broker))
    ], **settings)

    application.listen(8989)
    tornado.ioloop.IOLoop.current().start()
    
if __name__ == '__main__':
    start_server()
