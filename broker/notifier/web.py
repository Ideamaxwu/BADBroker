#!/usr/bin/env python3

#from BADWebServer import get_web_sockets

class WebClientNotifier():
    def __init__(self):
        self.live_web_sockets = set()

    def notify(self, userId, message):
        removable = set()

        for ws in self.live_web_sockets:
            if not ws.ws_connection or not ws.ws_connection.stream.socket:
                removable.add(ws)
            else:
                ws.write_message(message)
        for ws in removable:
            self.live_web_sockets.remove(ws)

    def set_live_web_sockets(self, live_web_sockets):
        self.live_web_sockets = live_web_sockets

    def __del__(self):
        pass