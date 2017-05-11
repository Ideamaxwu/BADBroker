#!/usr/bin/env python3

import simplejson as json
import tornado.httpclient
import urllib

gcm_server = 'https://fcm.googleapis.com/fcm/send'


client = tornado.httpclient.HTTPClient()
post_data = {'to': 'epYz7vfEmtA:APA91bFMQtyy5x3v69V7QQSHUpdSvMjgpQRtrvwq-oQwTCEhvZLdCGhbvKmLPuLwI5LG5XbQWEU7ca_GGH5RqKfJ9xx4To2zyKTWllkMoK-pGh65k_9k20byKFmHvWgkAaq2rFA_LLqm',
             'notification': {
                 'title': 'Test',
                 'text': 'This is text'
                },
             'data': {
                 'value': 'Hello'
                },
             'priority': 'high'
             }

#fayQ65shkAI:APA91bFcEVP9jphuOAOpkpDCbsKjKe8QsAtTsiSwP1Lwpzj3S4i07-BbmCtQe2r6IhexK5NtxpnMmo6b9X-d5TEKlS6oMQdSM2bX5rFiyltnJKOns4EwegUc4HSk_2ozQxglbQBsh9-Z

request = tornado.httpclient.HTTPRequest(gcm_server,
                                         method='POST',
                                         headers={'Content-Type': 'application/json',
                                                  'Authorization': 'key=AIzaSyB9OcjeeU0uwdqHtTh9XN-gTsvHL7v6v38'},
                                         body=json.dumps(post_data))  #{'registration_ids': ['ABC']}
response = client.fetch(request)
print(response)

print(response.code)
print(response.body)

