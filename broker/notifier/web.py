#!/usr/bin/env python3

#from BADWebServer import get_web_sockets
import tornado.gen
import brokerutils
import simplejson as json
import tornado.httpclient
import tornado.websocket

log = brokerutils.setup_logging(__name__)


class WebClientNotifier:
    def __init__(self):
        self.callbackUrls = {}
        self.live_web_sockets = set()
        self.websockets = {}

    def setCallbackUrl(self, dataverseName, userId, callbackUrl):
        if dataverseName not in self.callbackUrls:
            self.callbackUrls[dataverseName] = {}
        self.callbackUrls[dataverseName][userId] = callbackUrl

    @tornado.gen.coroutine
    def notify(self, dataverseName, userId, message):
        if dataverseName not in self.websockets or userId not in self.websockets[dataverseName]:
            log.error('User `%s` does not have an open websocket' % userId)
            return

        sockets = self.websockets[dataverseName][userId]
        removeable = set()

        for socket in sockets:
            try:
                socket.write_message(message)
            except tornado.websocket.WebSocketClosedError:
                log.error('Notification failed, websocket is closed for user `%s`' % userId)
                removeable.add(socket)

        # Remove the closed sockets
        if len(removeable) > 0:
            for socket in removeable:
                self.websockets[dataverseName][userId].remove(socket)
            if len(self.websockets[dataverseName][userId]) == 0:
                del self.websockets[dataverseName][userId]

    @tornado.gen.coroutine
    def notifyv2(self, dataverseName, userId, message):
        if dataverseName not in self.callbackUrls or userId not in self.callbackUrls[dataverseName]:
            log.error('User `%s` does not have a callback Url' % userId)
            return

        callbackUrl = self.callbackUrls[dataverseName][userId]
        try:
            request = tornado.httpclient.HTTPRequest(callbackUrl, method='POST', body=json.dumps(message))
            response = yield self.client.fetch(request)

            if response.code == 200:
                log.info('Web notification is sent to %s' % userId)
            else:
                log.info('Web notification to %s failed' % userId)

        except tornado.httpclient.HTTPError as e:
            log.error('Web notification failed ' + str(e))

    def notifyv1(self, dataverseName, userId, message):
        removable = set()

        for ws in self.live_web_sockets:
            if not ws.ws_connection or not ws.ws_connection.stream.socket:
                removable.add(ws)
            else:
                ws.write_message(message)
        for ws in removable:
            self.live_web_sockets.remove(ws)

    def addWebsocket(self, dataverseName, userId, websocket):
        if dataverseName not in self.websockets:
            self.websockets[dataverseName] = {}
        if userId not in self.websockets[dataverseName]:
            self.websockets[dataverseName][userId] = []

        self.websockets[dataverseName][userId].append(websocket)

    def set_live_web_sockets(self, live_web_sockets):
        self.live_web_sockets = live_web_sockets

    def __del__(self):
        pass