import badclient
import sys

def on_channelresults(channelName, subscriptionId, channelExecutionTime, resultCount):
    print(channelName, subscriptionId)
    print('Results for %s: result count %d, latest execution time %s' % (channelName, resultCount, channelExecutionTime))

    return

    results = client.getresults(channelName, subscriptionId, 100)
    while results and len(results) > 0:
        for item in results:
            print('APPDATA ' + str(item))

        returnedChannelExecutionTime = results['returnedChannelExecutionTime']

        client.ackresults(channelName, subscriptionId, returnedChannelExecutionTime)
        results = client.getresults(channelName, subscriptionId, 100)

def on_error(where, error_msg):
    print(where, ' ---> ', error_msg)

client = badclient.BADClient(brokerServer='localhost') #'cert24.ics.uci.edu')

dataverseName = sys.argv[1]
userName = sys.argv[2]
password = 'yusuf'
email = 'abc@abc.net'

client.on_channelresults = on_channelresults
client.on_error = on_error

client.register(dataverseName, userName, password, email)

if client.login() == False:
    print('Login failed')
    sys.exit(0)

client.listchannels()
client.listsubscriptions()

subcriptionId = client.subscribe('nearbyTweetChannel', ['man'])
print ('Subscribed with ID %s' %subcriptionId)

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
