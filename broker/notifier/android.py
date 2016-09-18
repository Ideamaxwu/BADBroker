#!/usr/bin/env python3
import tornado.httpclient
import logging as log
import tornado.gen
import simplejson as json

log.getLogger(__name__)
log.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=log.DEBUG)

class AndroidClientNotifier():
    def __init__(self):
        self.fcm_server = 'https://fcm.googleapis.com/fcm/send'

        # Authorization key is associated with bigactivedata@gmail.com, registered through FCM Console
        self.fcm_authorization_key = 'AIzaSyBAhgXCQERi2vwSdEEPrxvQV1xpJ7e4owk'

        # Mobile clients send their tokens and they are stored here
        self.fcm_registration_tokens = {}
        self.client = tornado.httpclient.AsyncHTTPClient()

    def setRegistrationToken(self, userId, registrationToken):
        log.info('Entering or Updating registration token of User %s' %userId)
        self.fcm_registration_tokens[userId] = registrationToken

    @tornado.gen.coroutine
    def notify(self, userId, message):
        if userId not in self.fcm_registration_tokens:
            log.error('User %s does not have an FCM token' %userId)
            return

        registration_token = self.fcm_registration_tokens[userId]
        post_data = {'to': registration_token,
                     'notification': {
                         'title':'New results',
                         'text': 'In channel %s' % message['channelName']
                     },
                     'data': message
                    }
        request = tornado.httpclient.HTTPRequest(self.fcm_server, method='POST',
                                                 headers={'Content-Type': 'application/json',
                                                          'Authorization:key': self.fcm_authorization_key},
                                                 body=json.dumps(post_data))
        response = yield self.client.fetch(request)
        log.info(response)

    def __del__(self):
        pass