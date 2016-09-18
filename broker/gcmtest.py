#!/usr/bin/env python3

import simplejson as json
import tornado.httpclient
import urllib

gcm_server = 'https://fcm.googleapis.com/fcm/send'


client = tornado.httpclient.HTTPClient()
post_data = {'to': 'fayQ65shkAI:APA91bFcEVP9jphuOAOpkpDCbsKjKe8QsAtTsiSwP1Lwpzj3S4i07-BbmCtQe2r6IhexK5NtxpnMmo6b9X-d5TEKlS6oMQdSM2bX5rFiyltnJKOns4EwegUc4HSk_2ozQxglbQBsh9-Z', 'data': {'value': 'Hello'}}

request = tornado.httpclient.HTTPRequest(gcm_server, method='POST',
                                         headers={'Content-Type': 'application/json',
                                                  'Authorization:key': 'AIzaSyBAhgXCQERi2vwSdEEPrxvQV1xpJ7e4owk'},
                                         body=json.dumps({'registration_ids': ['ABC']}))
response = client.fetch(request)
print(response)
