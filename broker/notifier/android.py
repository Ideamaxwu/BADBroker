#!/usr/bin/env python3

import tornado.httpclient
import logging as log
import tornado.gen
import simplejson as json

log.getLogger(__name__)
log.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=log.DEBUG)

class AndroidClientNotifier():
    def __init__(self):
        self.gcmServer = 'https://fcm.googleapis.com/fcm/send'

        # Authorization key is associated with bigactivedata@gmail.com, registered through FCM Console
        self.gcmAuthorizationKey = 'AIzaSyBAhgXCQERi2vwSdEEPrxvQV1xpJ7e4owk'

        # Mobile clients send their tokens and they are stored here
        self.gcmRegistrationTokens = {}
        self.client = tornado.httpclient.AsyncHTTPClient()

    def setRegistrationToken(self, userId, registrationToken):
        log.info('Entering or Updating registration token of User %s' %userId)
        self.gcmRegistrationTokens[userId] = registrationToken

    @tornado.gen.coroutine
    def notify(self, userId, message):
        if userId not in self.gcmRegistrationTokens:
            log.error('User %s does not have an FCM token' %userId)
            return

        registration_token = self.gcmRegistrationTokens[userId]
        post_data = {'to': registration_token,
                     'notification': {
                         'title':'New results',
                         'text': 'In channel %s' % message['channelName']
                     },
                     'data': message
                    }
        request = tornado.httpclient.HTTPRequest(self.gcmServer, method='POST',
                                                 headers={'Content-Type': 'application/json',
                                                          'Authorization': 'key=%s' %self.gcmAuthorizationKey},
                                                 body=json.dumps(post_data))
        response = yield self.client.fetch(request)
        log.info(response)

    def __del__(self):
        pass