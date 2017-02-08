import badclient
import sys

'''
def on_channelresults(channelName, subscriptionId, channelExecutionTime, resultCount):
    print(channelName, subscriptionId)
    print('Results for %s: result count %d, latest execution time %s' % (channelName, resultCount, channelExecutionTime))

    resultObject = client.getresults(channelName, subscriptionId, 10)

    while resultObject and len(resultObject['results']) > 0:
        latestChannelExecutionTimeInResults = resultObject['latestChannelExecutionTimeInResults']
        channelExecutionTime = resultObject['channelExecutionTime']

        results = resultObject['results']

        print('Retrieved %d results' % len(results))
        for item in results:
            print('APPDATA ' + str(item))

        client.ackresults(channelName, subscriptionId, latestChannelExecutionTimeInResults)
        resultObject = client.getresults(channelName, subscriptionId, 10)

'''

def on_channelresults(channelName, subscriptionId, results):
    print('Retrieved results for channel `%s` with sub `%s` -- %d records' %(channelName, subscriptionId, len(results)))
    for item in results:
        print('APPDATA ' + str(item))
    return True

def on_error(where, error_msg):
    print(where, ' ---> ', error_msg)


client = badclient.BADClient(brokerServer='%s.ics.uci.edu' % sys.argv[1])

dataverseName = sys.argv[2]
userName = sys.argv[3]
password = 'yusuf'
email = 'abc@abc.net'

client.on_channelresults = on_channelresults
client.on_error = on_error

client.register(dataverseName, userName, password, email)
subIds = []

if client.login():
    client.listchannels()
    subIds = client.listsubscriptions()
    print(subIds)
else:
    print('Registration or Login failed')
    sys.exit(0)

#if len(subIds) > 0:
#    client.unsubcribe(subIds[0]) # unsubscribing from the first the subscription

#subcriptionId = client.subscribe('nearbyTweetChannel', ['Dead'])
#print ('Subscribed with ID %s' % subcriptionId)

#client.subscribe('recentEmergenciesOfTypeChannel', ['tornado'], on_channelresults)
#client.insertrecords('TweetMessageuuids', [{'message-text': 'Happy man'}, {'message-text': 'Sad man'}])

# Feed created as per file 4
#result = client.callfunction('NearbyTweetsContainingText', ['man'])
#print(result)

'''
data = [{'recordId': str(random.random()),
        'userId': '237',
        'userName': '343434',
        'password': '12245',
        'email': 'value@abc.net'
        },
        {'recordId': str(random.random()),
        'userId': '9999',
        'userName': '343434',
        'password': '12245',
        'email': 'value@abc.net'
        }
        ]

client.feedrecords(10002, data)
'''

try:
    client.run() # blocking call
except KeyboardInterrupt:
    client.stop()
