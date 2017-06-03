#!/usr/bin/env python3

import re
import socket
from threading import Lock

from datetime import datetime
import tornado.gen
import tornado.httpclient
import tornado.ioloop
import tornado.iostream
import tornado.web

import notifier.android
import notifier.desktop
import notifier.web
from brokerobjects import *
from cachingschemes import lrucache

log = brokerutils.setup_logging(__name__)

#host = 'http://cacofonix-2.ics.uci.edu:19002'
#host = 'http://128.195.52.196:19002'
#host = 'http://45.55.22.117:19002/'
#host = 'http://128.195.52.76:19002'
#host = 'http://promethium.ics.uci.edu:19002'

mutex = Lock()
live_web_sockets = set()


class BADBroker:
    brokerInstance = None

    @classmethod
    def getInstance(cls):
        if BADBroker.brokerInstance is None:
            BADBroker.brokerInstance = BADBroker()
        return BADBroker.brokerInstance

    def __init__(self):
        self.asterix = AsterixQueryManager.getInstance()

        config = configparser.ConfigParser()
        config.read('brokerconfig.ini')

        if config.has_section('Broker'):
            self.brokerName = config.get('Broker', 'name')
            self.brokerIPAddr = config.get('Broker', 'ipaddress')
            self.brokerPort = config.get('Broker', 'port')
        else:
            self.brokerName = 'brokerF'  # self._myNetAddress()  # str(hashlib.sha224(self._myNetAddress()).hexdigest())
            self.brokerIPAddr = 'localhost'
            self.brokerPort = 8989

        if config.has_section('RabbitMQ'):
            self.rabbitMQServer = config.get('RabbitMQ', 'server')

        self.channelSubscriptionTable = {}  # indexed by dataverseName, channelname, channelSubscriptionId
        self.userSubscriptionTable = {}  # subscription indexed by dataverseName->channelName -> channelSubscriptionId -> userId

        self.userToSubscriptionMap = {}  # indexed by dataverseName, userId, userSubscriptionId

        self.sessions = {}                  # keep accesstokens of users currently logged in
        self.notifiers = {}                 # list of all possible notifiers

        self.initializeBroker()             # initialize broker, loads Users and ChannelSubscriptions
        self.initializeNotifiers()
        self.cache = lrucache.BADLRUCache(100)

        if config.has_section('BCS'):
            server = config.get('BCS', 'server')
            port = config.getint('BCS', 'port')
            self.bcsUrl = 'http://' + server + ':' + str(port)
        else:
            self.bcsUrl = 'http://radon.ics.uci.edu:5000'

        self.brokerIPAddr = self._myNetAddress()
        self.brokerName = 'Broker' + self.brokerIPAddr.replace('.', '') + 'P' +self.brokerPort

        #tornado.ioloop.IOLoop.current().add_callback(self._registerBrokerWithBCS)
        #tornado.ioloop.IOLoop.current().call_later(60, self.scheduleDropResultsFromChannels)

    @tornado.gen.coroutine
    def _registerBrokerWithBCS(self):

        post_request = {
                "brokerName": self.brokerName,
                "brokerIP": str(self.brokerIPAddr)
        }

        log.info(post_request)

        try:
            r = requests.post(self.bcsUrl + "/registerbroker", json=post_request)
            if r and r.status_code == 200:
                log.info('Broker registered successfully')
                log.info(r.text)
            else:
                log.debug('Broker registration with BCS failed, Exiting!!')
                exit(0)
        except Exception as e:
            log.debug('Broker registration with BCS failed for reason %s' %(e))
            log.debug('Exiting broker!')
            exit(0)

    def _myNetAddress(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 0))
        mylocaladdr = str(s.getsockname()[0])
        return mylocaladdr

    def initializeBroker(self):
        pass

    def initializeNotifiers(self):
        self.notifiers['desktop'] = notifier.desktop.DesktopClientNotifier()
        self.notifiers['android'] = notifier.android.AndroidClientNotifier()
        self.notifiers['web'] = notifier.web.WebClientNotifier()

    @tornado.gen.coroutine
    def register(self, dataverseName, userName, password, email):
        """
        Registers users/clients in the BAD broker.
        :param dataverseName: dataverse name where the user wants to join (where the application resides)
        :param userName: user name
        :param password: password for the account
        :param email: email address of the user
        :return: 'status': 'success'/'failed', if succeeded, returns 'userId', else 'error' contains error occurred
        """
        users = yield User.load(dataverseName=dataverseName, userName=userName)

        if users and len(users) > 0:
            user = users[0]
            log.warning('User %s is already registered' % (user.userId))

            return {
                'status': 'failed',
                'error': 'User is already registered with the same name!',
                'userId': user.userId
            }
        else:
            userId = str(hashlib.sha224((dataverseName + '@' + userName).encode()).hexdigest())  # '{}@{}'.format(userName, dataverseName)
            password = hashlib.sha224(password.encode()).hexdigest()

            user = User(dataverseName, userId, userId, userName, password, email)
            yield user.save()

            log.debug('Registered user %s with id %s' % (userName, userId))
            return {'status': 'success', 'userId': userId}

    @tornado.gen.coroutine
    def login(self, dataverseName, userName, password, platform):
        """
        Signs in user and creates a session for the user. Only after login, the user is able to perform operations.
        :param dataverseName: dataverse name
        :param userName: user name of the user/client
        :param password: password
        :param platform: platform on which the user in in the current login session. Can be 'desktop', 'web' and 'android'
        :return: On 'success', 'userId' and 'accessToken'
        """

        users = yield User.load(dataverseName=dataverseName, userName=userName)

        if users and len(users) > 0:
            user = users[0]
            password = hashlib.sha224(password.encode()).hexdigest()
            if password == user.password:
                accessToken = str(hashlib.sha224((dataverseName + userName + str(datetime.now())).encode()).hexdigest())
                userId = user.userId

                if dataverseName not in self.sessions:
                    self.sessions[dataverseName] = {}

                '''
                # Check if the user is already logged in, that is, has an entry in sessions
                if userId in self.sessions[dataverseName]:
                    return {
                        'status': 'failed',
                        'error': 'User `%s` is already logged in. Cannot have multiple sessions!' %userName
                    }
                '''
                # Create a new session for this user
                user.lastLoginTime = str(datetime.now())
                self.sessions[dataverseName][userId] = Session(dataverseName, userId, accessToken, platform, user,
                                                               datetime.now(), datetime.now())

                tornado.ioloop.IOLoop.current().add_callback(self.loadSubscriptionsForUser, dataverseName=dataverseName, userId=userId)
                return {'status': 'success', 'userName': userName, 'userId': userId, 'accessToken': accessToken}
            else:
                return {'status': 'failed', 'error': 'Password does not match!'}
        else:
            return {'status': 'failed', 'error': 'No user exists %s!' % userName}

    @tornado.gen.coroutine
    def loadSubscriptionsForUser(self, dataverseName, userId):
        log.info('Loading userSubscriptions for user `{0}`'.format(userId))
        userSubscriptions = yield UserSubscription.load(dataverseName, userId=userId)

        if userSubscriptions is None or len(userSubscriptions) == 0:
            return

        if dataverseName not in self.userToSubscriptionMap:
            self.userToSubscriptionMap[dataverseName] = {}
        if userId not in self.userToSubscriptionMap[dataverseName]:
            self.userToSubscriptionMap[dataverseName][userId] = {}

        # Fill userSubscriptions and userToSubscription maps
        for userSubscription in userSubscriptions:
            channelName = userSubscription.channelName
            userSubscriptionId = userSubscription.userSubscriptionId
            channelSubscriptionId = userSubscription.channelSubscriptionId

            # Check whether the channel subscription is in the table
            try:
                ch = self.channelSubscriptionTable[dataverseName][channelName][channelSubscriptionId]
            except KeyError as kerr:
                # The subscription is not in the table, so---
                #  Load the channel subscription
                #  Check if it is originated from this broker; if not, assign a existing (or new) channel subscription
                channelSubscriptions = yield ChannelSubscription.load(dataverseName, channelSubscriptionId=channelSubscriptionId)

                if not channelSubscriptions or len(channelSubscriptions) == 0:
                    log.error('Channel subscription is not found. Something WRONG!!!')
                    continue

                channelSubscription = channelSubscriptions[0]
                channelSubscriptionId = channelSubscription.channelSubscriptionId

                log.debug('Subscription found for `%s` at the channel `%s`'% (userSubscriptionId, channelSubscriptionId))

                if channelSubscription.brokerName != self.brokerName:
                    log.info('User `%s` might have switched the broker' % userId)
                    channelSubscription = yield self.checkExistingChannelSubscription(dataverseName, channelName, userSubscription.parameters)
                    if not channelSubscription:
                        channelSubscription = yield self.createChannelSubscription(dataverseName, channelName, userSubscription.parameters)

                    log.debug('New channelsubscription %s' % (channelSubscription.channelSubscriptionId))
                    channelSubscriptionId = channelSubscription.channelSubscriptionId

                    userSubscription.channelSubscriptionId = channelSubscriptionId
                    yield userSubscription.save()

                if dataverseName not in self.channelSubscriptionTable:
                    self.channelSubscriptionTable[dataverseName] = {}
                if channelName not in self.channelSubscriptionTable[dataverseName]:
                    self.channelSubscriptionTable[dataverseName][channelName] = {}
                self.channelSubscriptionTable[dataverseName][channelName][channelSubscriptionId] = channelSubscription

            # Populate subscription table and user-to-subscription map
            if dataverseName not in self.userSubscriptionTable:
                self.userSubscriptionTable[dataverseName] = {}
            if channelName not in self.userSubscriptionTable[dataverseName]:
                self.userSubscriptionTable[dataverseName][channelName] = {}
            if channelSubscriptionId not in self.userSubscriptionTable[dataverseName][channelName]:
                self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId] = {}
            self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId][userId] = userSubscription

            self.userToSubscriptionMap[dataverseName][userId][userSubscriptionId] = userSubscription

    @tornado.gen.coroutine
    def logout(self, dataverseName, userId, accessToken):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        self.clearStateForUser(dataverseName, userId)
        return {'status': 'success', 'userId': userId}

    @tornado.gen.coroutine
    def clearStateForUser(self, dataverseName, userId):
        # Update user state and delete session entry
        if dataverseName in self.sessions and userId in self.sessions[dataverseName]:
            user = self.sessions[dataverseName][userId].user
            user.lastLogoffTime = str(datetime.now())
            yield user.save()
            del self.sessions[dataverseName][userId]

        # Clear subscription from user-to-subscription map
        if dataverseName in self.userToSubscriptionMap and userId in self.userToSubscriptionMap[dataverseName]:
            del self.userToSubscriptionMap[dataverseName][userId]

        # Clear subscriptions from the subscription Table
        if dataverseName in self.userSubscriptionTable:
            for channelName in self.userSubscriptionTable[dataverseName]:
                for channelSubscriptionId in self.userSubscriptionTable[dataverseName][channelName]:
                    if userId in self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId]:
                        del self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId][userId]

    @tornado.gen.coroutine
    def createChannelSubscription(self, dataverseName, channelName, parameters):
        result = yield self.getChannelInfo(dataverseName, channelName)

        if result['status'] == 'failed':
            raise BADException('Retrieving channel info failed')

        channels = result['channels']

        log.info(channels)

        if len(channels) == 0:
            log.warning('No channel exits with name %s' % channelName)
            raise BADException('No channel exists with name %s!' % channelName)

        channel = channels[0]

        log.info(channel['Function'])
        log.info(channel['ChannelName'])
        log.info(channel['ResultsDatasetName'])

        function_name = channel['Function']
        arity = int(function_name.split('@')[1])

        '''
        if arity != len(parameters):
            log.warning('Channel routine takes %d arguments, given %d' % (arity, len(parameters)))
            raise BADException('Channel routine takes %d arguments, given %d' % (arity, len(parameters)))


        parameter_list = ''
        if parameters is not None:
            for value in parameters:
                if len(parameter_list) != 0:
                    parameter_list = parameter_list + ', '

                if isinstance(value, str):
                    parameter_list = parameter_list + '\"' + value + '\"'
                else:
                    parameter_list = parameter_list + str(value)
        '''

        aql_stmt = 'subscribe to ' + channelName + '(' + parameters + ') on ' + self.brokerName
        log.debug(aql_stmt)

        status_code, response = yield self.asterix.executeSQLPP(dataverseName, aql_stmt)

        if status_code != 200:
            log.debug(response)
            if 'There is no broker with this name' in response:
                log.info('Broker `%s` is not registered. Registering..' % self.brokerName)
                command = 'create broker {} at "http://{}:{}/notifybroker"'.format(self.brokerName, self.brokerIPAddr, self.brokerPort)
                status_code, response = yield self.asterix.executeSQLPP(dataverseName, command)
                if status_code != 200:
                    raise BADException(response)
                else:
                    status_code, response = yield self.asterix.executeSQLPP(dataverseName, aql_stmt)
                    if status_code != 200:
                        raise BADException(response)
            else:
                raise BADException(response)

        response = response.replace('\n', '').replace(' ', '')
        log.debug(response)

        # response = json.loads(response)
        channelSubscriptionId = re.match(r'\[uuid\(\"(.*)\"\)\]', response).group(1)

        uniqueId = dataverseName + '::' + channelName + '::' + channelSubscriptionId
        currentDateTime = yield self.getCurrentDateTime()
        channelSubscription = ChannelSubscription(dataverseName, uniqueId, channelName, self.brokerName, parameters, channelSubscriptionId, currentDateTime)
        yield channelSubscription.save()

        return channelSubscription

    @tornado.gen.coroutine
    def createUserSubscription(self, dataverseName, userId, channelName, parameters, channelSubscriptionId, timestamp):
        resultsDataset = channelName + 'Results'

        userSubscriptionId = self.makeUserSubscriptionId(dataverseName, channelName, parameters, userId)

        userSubscription = UserSubscription(dataverseName, userSubscriptionId, self.brokerName, userSubscriptionId, userId,
                                    channelSubscriptionId, channelName, parameters, timestamp, resultsDataset)
        yield userSubscription.save()

        if channelName not in self.userSubscriptionTable[dataverseName]:
            self.userSubscriptionTable[dataverseName][channelName] = {}

        if channelSubscriptionId not in self.userSubscriptionTable[dataverseName][channelName]:
            self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId] = {}

        log.info(self.userSubscriptionTable[dataverseName])
        self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId][userId] = userSubscription

        if dataverseName not in self.userToSubscriptionMap:
            self.userToSubscriptionMap[dataverseName] = {}
        if userId not in self.userToSubscriptionMap[dataverseName]:
            self.userToSubscriptionMap[dataverseName][userId] = {}
        self.userToSubscriptionMap[dataverseName][userId][userSubscriptionId] = userSubscription

        return userSubscription

    @tornado.gen.coroutine
    def checkExistingChannelSubscription(self, dataverseName, channelName, parameters):
        parametersHash = str(hashlib.sha224((str(parameters)).encode()).hexdigest())
        channelSubscriptions = yield ChannelSubscription.load(dataverseName, channelName=channelName,
                                                              brokerName=self.brokerName, parametersHash=parametersHash)
        if channelSubscriptions and len(channelSubscriptions) > 0:
            if len(channelSubscriptions) > 1:
                log.debug('Multiple subscriptions matched, picking 0-th')
            return channelSubscriptions[0]
        else:
            return None

    @tornado.gen.coroutine
    def getCurrentDateTime(self):
        status, response = yield self.asterix.executeQuery(None, "let $t := current-datetime() return $t")
        if status != 200:
            return None

        log.debug(response)
        currentDateTime = json.loads(response)[0]  # re.match(r'\[\"(.*)\"\]', response).group(1)

        log.debug('Current time at server ' + currentDateTime)
        return currentDateTime

    @tornado.gen.coroutine
    def subscribe(self, dataverseName, userId, accessToken, channelName, parameters):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        if dataverseName not in self.channelSubscriptionTable:
            self.channelSubscriptionTable[dataverseName] = {}

        if dataverseName not in self.userSubscriptionTable:
            self.userSubscriptionTable[dataverseName] = {}

        if dataverseName not in self.userToSubscriptionMap:
            self.userToSubscriptionMap[dataverseName] = {}
        if userId not in self.userToSubscriptionMap[dataverseName]:
            self.userToSubscriptionMap[dataverseName][userId] = {}

        # Convert parameters (a list) to a string
        parameters = json.dumps(parameters).replace('[','').replace(']','')

        # Check whether the channel already has a subscription with the same param values
        try:
            channelSubscription = yield self.checkExistingChannelSubscription(dataverseName, channelName=channelName,
                                                                              parameters=parameters)
            if channelSubscription:
                log.info('Subscription found, id = %s, channel %s with params %s' % (channelSubscription.channelSubscriptionId,
                                                                                     channelName, parameters))
            else:
                log.debug('Creating subscription, for channel %s with params %s' % (channelName, parameters))
                channelSubscription = yield self.createChannelSubscription(dataverseName, channelName, parameters)

            channelSubscriptionId = channelSubscription.channelSubscriptionId
            if channelName not in self.channelSubscriptionTable[dataverseName]:
                self.channelSubscriptionTable[dataverseName][channelName] = {}

            self.channelSubscriptionTable[dataverseName][channelName][channelSubscriptionId] = channelSubscription

        except BADException as badex:
            log.error(badex)
            return {'status': 'failed', 'error': str(badex)}

        currentDateTime = yield self.getCurrentDateTime()
        userSubscription = yield self.createUserSubscription(dataverseName, userId, channelName, parameters, channelSubscriptionId, currentDateTime)

        if userSubscription:
            return {'status': 'success', 'userSubscriptionId': userSubscription.userSubscriptionId, 'timestamp': currentDateTime}
        else:
            return {'status': 'failed', 'error': 'User subscription creation failed!!'}

    @tornado.gen.coroutine
    def unsubscribe(self, dataverseName, userId, accessToken, userSubscriptionId):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        try:
            userSubscription = self.userToSubscriptionMap[dataverseName][userId][userSubscriptionId]
        except KeyError as kerr:
            log.warning('No such subscription %s' % userSubscriptionId)
            return {'status': 'failed', 'error': 'No such subscription %s' % userSubscriptionId}

        channelSubscriptionId = userSubscription.channelSubscriptionId
        channelName = userSubscription.channelName

        del self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId][userId]
        del self.userToSubscriptionMap[dataverseName][userId][userSubscriptionId]

        yield userSubscription.delete()

        log.info('User %s unsubscribed from %s' % (userId, channelName))

        # Check if there is other subscriptions left for this channel from this broker
        userSubscriptions = yield UserSubscription.load(dataverseName, channelSubscriptionId=channelSubscriptionId)
        if not userSubscriptions or len(userSubscriptions) == 0:
            # No subscription left for this channel, so unsubscribe and remove the subscription
            status_code, response = yield self.asterix.executeSQLPP(dataverseName, 'unsubscribe \"{0}\" from {1}'.format(channelSubscriptionId, channelName))
            if status_code != 200:
                raise BADException(response)

            del self.channelSubscriptionTable[dataverseName][channelName][channelSubscriptionId]
            yield ChannelSubscription.deleteWhere(dataverseName, brokerName=self.brokerName,
                                             channelName=channelName, channelSubscriptionId=channelSubscriptionId)

        return {'status': 'success'}

    def makeUserSubscriptionId(self, dataverseName, channelName, parameters, userId):
        sid = dataverseName + '::' + userId + '::' + channelName + '::' + parameters.replace(', ', '').replace('"', '')
        return str(hashlib.sha224(sid.encode()).hexdigest())

    @tornado.gen.coroutine
    def getResults(self, dataverseName, userId, accessToken, userSubscriptionId, channelExecutionTime, resultSize=0):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        try:
            userSubscription = self.userToSubscriptionMap[dataverseName][userId][userSubscriptionId]
        except KeyError as kerr:
            msg = 'No subscription %s is found for user %s' % (userSubscriptionId, userId)
            log.warning(msg)
            return {'status': 'failed', 'error': msg}

        channelName = userSubscription.channelName
        channelSubscriptionId = userSubscription.channelSubscriptionId

        # If not channelExecutionTime is provided, obtain the latest one from the channel
        if not channelExecutionTime:
            channelExecutionTime = self.channelSubscriptionTable[dataverseName][channelName][channelSubscriptionId].latestChannelExecutionTime

        # retrieve user subscription for this channel
        userSubscription = self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId][userId]
        latestDeliveredResultTime = userSubscription.latestDeliveredResultTime

        log.debug('Check %s --- %s' %(latestDeliveredResultTime, channelExecutionTime))

        # Retrieve all execution times from current to the last delivered time
        whereClause = 'subscriptionId = uuid(\"{0}\") ' \
                      'and channelExecutionTime > datetime(\"{1}\") ' \
                      'and channelExecutionTime <= datetime(\"{2}\")'.format(channelSubscriptionId,
                                                                                latestDeliveredResultTime,
                                                                                channelExecutionTime)

        orderbyClause = 'channelExecutionTime asc'
        aql_stmt = 'SELECT * FROM %s ' \
                   'distinct by $t.channelExecutionTime ' \
                   'WHERE %s ' \
                   'ORDER BY %s ' \
                   '%s' \
                   % ((channelName + 'Results'), whereClause, orderbyClause,
                      'LIMIT {}'.format(resultSize) if resultSize and resultSize > 0 else '')

        status, response = yield self.asterix.executeQuery(dataverseName, aql_stmt)

        log.debug('Results status {0} response {1}'.format(status, response))

        if status != 200:
            log.error('Execution time retrieval failed.')
            return {'status': 'failed', 'error': 'Execution time retrieval failed'}

        channelExecutionTimes = json.loads(response)

        if channelExecutionTimes and len(channelExecutionTimes) > 0:
            resultToUser = []

            # Currently returns all pending results
            for channelExecutionTimeToReturn in channelExecutionTimes:
                # First check in the cache, if not retrieve from the Asterix
                resultFromCache = self.getResultsFromCache(dataverseName, channelName, channelSubscriptionId, channelExecutionTimeToReturn)

                if resultFromCache:
                    log.info('Cache HIT for %s' % (self.getResultKey(dataverseName, channelName, channelSubscriptionId, channelExecutionTimeToReturn)))
                    log.debug(resultFromCache)
                    resultToUser.extend(resultFromCache)
                else:
                    log.info('Cache MISS for %s' % (self.getResultKey(dataverseName, channelName, channelSubscriptionId, channelExecutionTimeToReturn)))
                    resultFromAsterix = yield self.getResultsFromAsterix(dataverseName, channelName, channelSubscriptionId, channelExecutionTimeToReturn)

                    if resultFromAsterix:
                        resultToUser.extend(resultFromAsterix)

                    '''
                    # Cache the results
                    if resultFromAsterix:
                        self.putResultsIntoCache(dataverseName, channelName, channelSubscriptionId, channelExecutionTimeToReturn, resultFromAsterix)
                        log.debug(resultFromAsterix)
                    '''

            return {'status': 'success',
                    'channelName': channelName,
                    'userSubscriptionId': userSubscriptionId,
                    'channelExecutionTime': channelExecutionTime,
                    'latestChannelExecutionTimeInResults': channelExecutionTimes[-1],
                    'results': resultToUser}
        else:
            return {'status': 'failed', 'error': 'No result to retrieve'}

    @tornado.gen.coroutine
    def getLatestResults(self, dataverseName, userId, accessToken, channelName, userSubscriptionId):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        try:
            userSubscription = self.userToSubscriptionMap[dataverseName][userId][userSubscriptionId]
        except KeyError as kerr:
            msg = 'No subscription %s is found for user %s' % (userSubscriptionId, userId)
            log.warning(msg)
            return {'status': 'failed', 'error': msg}

        channelName = userSubscription.channelName
        channelSubscriptionId = userSubscription.channelSubscriptionId
        latestChannelExecutionTime = self.channelSubscriptionTable[dataverseName][channelName][channelSubscriptionId].latestChannelExecutionTime

        # Retrieve the last execution time of this subscription
        whereClause = 'subscriptionId = uuid(\"{}\") ' \
                      'and channelExecutionTime = datetime(\"{}\")'.format(channelSubscriptionId, latestChannelExecutionTime)

        aql_stmt = 'SELECT result.result FROM %s result WHERE %s' % ((channelName + 'Results'), whereClause)

        status, response = yield self.asterix.executeQuery(dataverseName, aql_stmt)

        if status == 200 and response:
            return {
                    'status': 'success',
                    'channelName': channelName,
                    'userSubscriptionId': userSubscriptionId,
                    'channelExecutionTime': latestChannelExecutionTime,
                    'results': json.loads(response)
                }
        else:
            log.error('Getlatestresult failed ' + response)
            return {
                'status': 'failed',
                'error': 'No new results in the channel'
            }

    @tornado.gen.coroutine
    def ackResults(self, dataverseName, userId, accessToken, userSubscriptionId, channelExecutionTime):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        try:
            userSubscription = self.userToSubscriptionMap[dataverseName][userId][userSubscriptionId]
        except KeyError as kerr:
            msg = 'No subscription %s is found for user %s' % (userSubscriptionId, userId)
            log.warning(msg)
            return {'status': 'failed', 'error': msg}

        # Update the latest delivered result timestamp of this subscription
        userSubscription.latestDeliveredResultTime = channelExecutionTime
        yield userSubscription.save()
        return {'status': 'success'}

    def getResultKey(self, dataverseName, channelName, channelSubscriptionId, channelExecutionTime):
        return dataverseName + '::' + channelName + '::' + channelSubscriptionId + '::' + channelExecutionTime

    def getResultsFromCache(self, dataverseName, channelName, channelSubscriptionId, channelExecutionTime):
        resultKey = self.getResultKey(dataverseName, channelName, channelSubscriptionId, channelExecutionTime)
        cachedResults = self.cache.get(resultKey)

        if cachedResults:
            if isinstance(cachedResults, str) or isinstance(cachedResults, bytes):
                return json.loads(str(cachedResults, encoding='utf-8'))
            else:
                return cachedResults
        else:
            return None

    def putResultsIntoCache(self, dataverseName, channelName, channelSubscriptionId, channelExecutionTime, results):
        resultKey = self.getResultKey(dataverseName, channelName, channelSubscriptionId, channelExecutionTime)

        if self.cache.put(resultKey, results):
            log.info('Results %s cached' % resultKey)
        else:
            log.warning('Results %s caching failed' % resultKey)

    @tornado.gen.coroutine
    def getResultsFromAsterix(self, dataverseName, channelName, channelSubscriptionId, channelExecutionTime):
        #return [{'channelExecutionTime': channelExecutionTime, 'result': ['A', 'B', 'C']}]

        whereClause = 'subscriptionId = uuid(\"{0}\") ' \
                      'and channelExecutionTime = datetime(\"{1}\")'.format(channelSubscriptionId, channelExecutionTime)
        orderbyClause = 'channelExecutionTime asc'
        aql_stmt = 'SELECT * FROM %s where %s ORDER BY %s' % ((channelName + 'Results'), whereClause, orderbyClause)

        status, response = yield self.asterix.executeQuery(dataverseName, aql_stmt)

        log.debug('Results status %d response %s' % (status, response))

        if status == 200:
            if response:
                response = response.replace('\n', '')
                results = json.loads(response)

                resultToUser = []
                for item in results:
                    resultToUser.append(
                        {
                            'channelExecutionTime': item['channelExecutionTime'],
                            'resultId': item['resultId'],
                            'result': item['result']
                        })
                return resultToUser
            else:
                return None
        else:
            return None

    @tornado.gen.coroutine
    def listchannels(self, dataverseName, userId, accessToken):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        aql_stmt = 'for $channel in dataset Metadata.Channel ' \
                   'where $channel.DataverseName = \"{}\" ' \
                   'return $channel'.format(dataverseName)

        status, response = yield self.asterix.executeQuery(dataverseName, aql_stmt)

        if status == 200:
            response = response.replace('\n', '')
            print(response)

            channels = json.loads(response)

            return {'status': 'success', 'channels': channels}
        else:
            return {'status': 'failed', 'error': response}

    @tornado.gen.coroutine
    def getChannelInfo(self, dataverseName, channelName):
        aql_stmt = 'SELECT * FROM Metadata.Channel '
        aql_stmt = aql_stmt + 'WHERE ChannelName = \"' + channelName + '\" '

        log.debug(aql_stmt)

        status, response = yield self.asterix.executeQuery(dataverseName, aql_stmt)

        if status == 200:
            response = response.replace('\n', '')
            print(response)
            channels = json.loads(response, encoding='utf-8')
            return {'status': 'success', 'channels': channels}
        else:
            return {'status': 'failed', 'error': response}

    @tornado.gen.coroutine
    def listsubscriptions(self, dataverseName, userId, accessToken):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        userSubscriptions = yield UserSubscription.load(dataverseName=dataverseName, userId=userId)
        print(userSubscriptions)

        return {'status': 'success', 'subscriptions': userSubscriptions if userSubscriptions else []}

    @tornado.gen.coroutine
    def notifyBroker(self, dataverseName, channelName, channelExecutionTime, channelSubscriptionIds):
        # if brokerName != self.brokerName:
        #    return {'status': 'failed', 'error': 'Not the intended broker %s' %(brokerName)}

        # Register a callback to retrieve results for this notification and notify all users
        tornado.ioloop.IOLoop.current().add_callback(self.retrieveLatestResultsAndNotifyUsers, dataverseName,
                                                     channelName, channelExecutionTime, channelSubscriptionIds)
        return {'status': 'success'}

    @tornado.gen.coroutine
    def retrieveLatestResultsAndNotifyUsers(self, dataverseName, channelName, channelExecutionTime, channelSubscriptionIds):
        try:
            log.debug('Current instances on the channel: %s' % self.channelSubscriptionTable[dataverseName][channelName])
        except KeyError as kerr:
            log.error('No dataverse `%s` or no active subscriptions on channel `%s`' %(dataverseName, channelName))
            return

        # Retrieve the latest delivery times for the subscriptions in channelSubscriptionIds
        for channelSubscriptionId in channelSubscriptionIds:
            if channelSubscriptionId not in self.channelSubscriptionTable[dataverseName][channelName]:
                log.warning('Notification for a non-existent channelsubscription %s on `%s`' % (channelSubscriptionId, channelName))
                continue

            # set latestChannelExecutionTime to the channel
            latestChannelExecutionTime = self.channelSubscriptionTable[dataverseName][channelName][channelSubscriptionId].latestChannelExecutionTime

            self.channelSubscriptionTable[dataverseName][channelName][channelSubscriptionId].latestChannelExecutionTime = channelExecutionTime
            yield self.channelSubscriptionTable[dataverseName][channelName][channelSubscriptionId].save()

            # Check if whether there is any subscription for this channel and any of them are active
            atleastOneActive = False
            try:
                atleastOneActive = any(userId in self.sessions[dataverseName]
                                       for userId in self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId])

            except KeyError as kerr:
                log.debug('No user subscribed to this channel %s--%s' % (channelName, channelSubscriptionId))
                pass

            # Skip if no user has an active session
            if not atleastOneActive:
                log.debug('No user has an active session on %s--%s' % (channelName, channelSubscriptionId))
                continue

            # Now, retrieve results and try to cache them
            query = 'SELECT channelExecutionTime FROM {0}Results ' \
                    'distinct by channelExecutionTime ' \
                    'WHERE subscriptionId = uuid(\"{1}\") ' \
                    'and channelExecutionTime > datetime(\"{2}\") ' \
                    'and channelExecutionTime <= datetime(\"{3}\") ' \
                    'order by channelExecutionTime'.format(channelName,
                                                    channelSubscriptionId,
                                                    latestChannelExecutionTime, channelExecutionTime)

            log.debug(query)

            status, response = yield self.asterix.executeQuery(dataverseName, query)
            log.debug(response)

            latestChannelExecutionTimes = []
            log.debug('ChannelName' + channelName + ' subscriptionId' + channelSubscriptionId + ' ' + str(latestChannelExecutionTimes))

            if status == 200 and response:
                latestChannelExecutionTimes = json.loads(response)

                if latestChannelExecutionTimes and len(latestChannelExecutionTimes) > 0:
                    log.info('Channel %s Latest delivery time %s' %(channelName, latestChannelExecutionTimes))

                    # Retrieve results from the Asterix and cache them
                    for latestChannelExecutionTime in latestChannelExecutionTimes:
                        results = yield self.getResultsFromAsterix(dataverseName, channelName, channelSubscriptionId, latestChannelExecutionTime)
                        resultKey = self.getResultKey(dataverseName, channelName, channelSubscriptionId, latestChannelExecutionTime)
                        if results and not self.cache.hasKey(resultKey):
                            self.putResultsIntoCache(dataverseName, channelName, channelSubscriptionId, latestChannelExecutionTime, results)

                    # Send notifications to all users who subscribed to this channel, ONLY the latest executionTime is notified
                    tornado.ioloop.IOLoop.current().add_callback(self.notifyAllUsers,
                                                                 dataverseName=dataverseName,
                                                                 channelName=channelName,
                                                                 channelSubscriptionId=channelSubscriptionId,
                                                                 latestChannelExecutionTime=latestChannelExecutionTime)
                else:
                    log.error('No new results to retrieve from channel %s' % channelName)
            else:
                    log.error('Retrieving delivery time failed for channel %s' % channelName)

    @tornado.gen.coroutine
    def notifyAllUsers(self, dataverseName, channelName, channelSubscriptionId, latestChannelExecutionTime):
        log.info('Sending out notification for channel %s subscription %s channelExecutionTime %s' % (channelName,
                                                                                              channelSubscriptionId,
                                                                                              latestChannelExecutionTime))
        for userId in self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId]:
            sub = self.userSubscriptionTable[dataverseName][channelName][channelSubscriptionId][userId]
            userSubcriptionId = sub.userSubscriptionId
            latestDeliveredResultTime = sub.latestDeliveredResultTime
            resultCount = yield self.getResultCount(dataverseName, channelName, userId, channelSubscriptionId, latestDeliveredResultTime, latestChannelExecutionTime)
            yield self.notifyUser(dataverseName, channelName, userId, channelSubscriptionId, userSubcriptionId,
                                  latestChannelExecutionTime, resultCount)

    @tornado.gen.coroutine
    def getResultCount(self, dataverseName, channelName, userId, channelSubscriptionId, latestDeliveredResultTime, latestChannelExecutionTime):
        statement = "SELECT count(distinct channelExecutionTime) FROM {}Results " \
                    "WHERE subscriptionId = uuid(\"{}\") and " \
                    "channelExecutionTime > datetime(\"{}\") and " \
                    "channelExecutionTime <= datetime(\"{}\")".format(channelName, channelSubscriptionId, latestDeliveredResultTime, latestChannelExecutionTime)

        status, response = yield self.asterix.executeQuery(dataverseName, statement)
        if status == 200 and response:
            resultCount = json.loads(response)[0]
            return resultCount
        else:
            return -1 # Something wrong

    @tornado.gen.coroutine
    def notifyUser(self, dataverseName, channelName, userId, channelSubscriptionId, userSubscriptionId, latestChannelExecutionTime, resultCount):
        log.info('Channel %s: sending notification to user %s for %s with resultcount %d' % (channelName, userId, userSubscriptionId, resultCount))

        message = {'userId': userId,
                   'dataverseName': dataverseName,
                   'channelName':  channelName,
                   'channelSubscriptionId': channelSubscriptionId,
                   'userSubscriptionId': userSubscriptionId,
                   'resultCount': resultCount,
                   'channelExecutionTime': latestChannelExecutionTime
                   }

        if dataverseName not in self.sessions or userId not in self.sessions[dataverseName]:
            log.error('User %s is not logged in to receive notifications' %userId)
        else:
            platform = self.sessions[dataverseName][userId].platform
            if platform not in self.notifiers:
                log.error('Platform `%s` is NOT supported yet!!' % platform)
            else:
                if platform == 'web':
                    mutex.acquire()
                    try:
                        global live_web_sockets
                        self.notifiers[platform].set_live_web_sockets(live_web_sockets)
                    finally:
                        mutex.release()

                if platform == 'android':
                    yield self.notifiers[platform].notify(userId, message)
                else:
                    self.notifiers[platform].notify(userId, message)

    @tornado.gen.coroutine
    def moveSubscription(self, channelSubscriptionId, channelName, brokerB):
        # move subscription "c45ef6d0-c5ae-4b9e-b5da-cf1932718296" on nearbyTweetChannel to BrokerB
        aql_stmt = 'move subscription \"{0}\" on {1} to {2}'.format(channelSubscriptionId, channelName, brokerB)
        status_code, response = yield self.asterix.executeSQLPP(aql_stmt)

        if status_code != 200:
            raise BADException(response)
        log.info('Subscription {0} on channel {1} moved to broker {2}'.format(channelSubscriptionId, channelName, brokerB))

    @tornado.gen.coroutine
    def callFunction(self, dataverseName, userId, accessToken, functionName, parameters):
        """
        :param dataverseName: dataverse name
        :param userId: userId
        :param accessToken: access token
        :param functionName: function to be called
        :param args: arguments need to be passed to the call
        :return: results
        """

        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        params = ''
        for value in parameters:
            if len(params) > 0:
                params = params + ', '
            if isinstance(value, str):
                params = params + '\"{}\"'.format(value)
            else:
                params = params + "{}".format(value)

        aql_stmt = 'SELECT * FROM {}({})'.format(functionName, params)
        status_code, response = yield self.asterix.executeSQLPP(dataverseName, aql_stmt)

        if status_code == 200 and response:
            result = json.loads(response)
            return {
                'status': 'success',
                'results': result
            }
        else:
            return {
                'status': 'failed',
                'error': 'Call execution failed ' + response
            }

    @tornado.gen.coroutine
    def insertRecords(self, dataverseName, userId, accessToken, datasetName, records):
        #check = self._checkAccess(dataverseName, userId, accessToken)
        #if check['status'] == 'failed':
        #    return check

        aql_stmt = 'INSERT into {0}({1})'.format(datasetName, records)
        status_code, response = yield self.asterix.executeSQLPP(dataverseName, aql_stmt)

        if status_code != 200:
            raise BADException(response)

        log.info('Records added into %s' %datasetName)
        return {'status': 'success'}

    @tornado.gen.coroutine
    def feedRecords(self, dataverseName, userId, accessToken, portNo, records):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            iostream = tornado.iostream.IOStream(socket=sock)
            yield iostream.connect((self.asterix.asterix_server, portNo))

            if isinstance(records, list):
                for record in records:
                    log.info('Feeding record {0}'.format(record))
                    yield iostream.write(json.dumps(record).encode('utf-8'))
            else:
                record = records
                log.info('Feeding record {0}'.format(record))
                yield iostream.write(json.dumps(record).encode('utf-8'))

        except Exception as e:
            return {'status': 'failed', 'error': 'Feed failed, may be due to ' + str(e)}
        finally:
            if iostream:
                iostream.close()
            if sock:
                sock.close()

        return {'status': 'success'}

    def gcmRegistration(self, dataverseName, userId, accessToken, gcmRegistrationToken):
        check = self._checkAccess(dataverseName, userId, accessToken)
        if check['status'] == 'failed':
            return check

        self.notifiers['android'].setRegistrationToken(userId, gcmRegistrationToken)
        return {'status': 'success'}

    @tornado.gen.coroutine
    def scheduleDropResultsFromChannels(self):
        yield self.dropChannelResults()
        tornado.ioloop.IOLoop.current().call_later(60, self.scheduleDropResultsFromChannels)

    @tornado.gen.coroutine
    def dropChannelResults(self, **kwargs):
        """
        Drop records from channelresults upto the mark behind which all subscribers consumed the records
        :return:
        """

        if kwargs and 'dataverse' in kwargs and 'channelSubscriptionId' in kwargs:
            dataverse = kwargs['dataverse']
            channelSubscriptionId = kwargs['channelSubscriptionId']
            latestResultDeliveryTime = kwargs['latestResultDeliveryTime']

            channels = yield ChannelSubscription.load(dataverse, channelSubscriptionId=channelSubscriptionId)
            if channels and len(channels) > 0:
                channelName = channels[0].channelName

                # Find the number of records to be deleted
                statement = 'SELECT count(*) FROM {}Results ' \
                            'where subscriptionId = uuid("{}") ' \
                            'and channelExecutionTime <= datetime("{}")'.format(channelName, channelSubscriptionId, latestResultDeliveryTime)

                status, response = yield self.asterix.executeQuery(dataverse, statement)

                if status == 200 and response:
                    count = json.loads(response)[0]
                    if count > 0:
                        statement = 'DELETE FROM {}Results ' \
                                    'where subscriptionId = uuid("{}") ' \
                                    'and channelExecutionTime <= datetime(\"{}\")'.format(channelName, channelSubscriptionId,
                                                                                             latestResultDeliveryTime)

                        status, response = yield self.asterix.executeSQLPP(dataverse, statement)

                        if status == 200:
                            log.info('%d resultsets are deleted from Channel %s with ChannelSubscriptionId %s' %(count,
                                                                                                             channelName,
                                                                                                             channelSubscriptionId))
                        else:
                            log.error('ChannelSubscription results deletion is failed')
                    else:
                        log.error('ChannelSubscription NO results to delete')
            else:
                log.error('Channel with id %s could not be found' %channelSubscriptionId)

        elif kwargs and 'dataverse' in kwargs:
            dataverse = kwargs['dataverse']

            # Find min(latestDeliveryTime) for each subscription
            statement = 'SELECT value {\"channelSubscriptionId\": channelSubscriptionId, \"latestDeliveredResultTime\": min(latestDeliveredResultTime)} ' \
                        'FROM UserSubscriptionDataset ' \
                        'group by channelSubscriptionId'

            status, response = yield self.asterix.executeSQLPP(dataverse, statement)

            if status == 200 and response:
                log.debug(response)
                channelinfo = json.loads(response)
                for channel in channelinfo:
                    channelSubscriptionId = channel['channelSubscriptionId']
                    latestResultDeliveryTime = channel['latestDeliveredResultTime']
                    log.info('ChannelSubscriptionId %s has min(delivered result time) %s' %(channelSubscriptionId, latestResultDeliveryTime))
                    yield self.dropChannelResults(dataverse=dataverse, channelSubscriptionId=channelSubscriptionId,
                                            latestResultDeliveryTime=latestResultDeliveryTime)
            else:
                log.error('ChannelSubscriptionId failed.')
        else:
            # Find all dataverses from Metadata.Channel
            statement = 'SELECT DataverseName FROM Metadata.Channel distinct by DataverseName'
            status, response = yield self.asterix.executeQuery(None, statement)

            if status == 200 and response:
                dataverses = json.loads(response)
                for dataverse in dataverses:
                    yield self.dropChannelResults(dataverse=dataverse)

    def _checkAccess(self, dataverseName, userId, accessToken):
        try:
            session = self.sessions[dataverseName][userId]
        except KeyError as kerr:
            return {
                'status': 'failed',
                'error': 'User is not authenticated'
            }

        if accessToken == session.accessToken:
            session.lastAccessedTime = datetime.now()
            return {'status': 'success'}
        else:
            return {
                'status': 'failed',
                'error': 'Invalid access token'
            }

    # Application Management routines
    @tornado.gen.coroutine
    def registerApplication(self, appName, appDataverse, adminUser, adminPassword, email, dropExisting=0, setupAQL=None):
        log.info('Check application environment, check whether \'BrokerMetadata\' dataverse exists, if not, create')
        yield Application.setupApplicationEnviroment(self.asterix)

        # Check if there is already an app exist with the same name, currently ignored.
        if dropExisting == 0:
            applications = yield Application.load(dataverseName=Application.dataverseName, appName=appName)
            if applications and len(applications) > 0:
                log.info('Obtained application with the same name `{}` in dataverse `{}`'.format(appName, appDataverse))
                return {
                    'status': 'failed',
                    'error': 'Application exists with the same name `{}` in the same dataverse `{}`'.format(appName, appDataverse)
                }

        log.info('Registering fresh application `{}` at dataverse `{}`'.format(appName, appDataverse))

        command = 'drop dataverse {} if exists; create dataverse {};'.format(appDataverse, appDataverse)
        status, response = yield self.asterix.executeSQLPP(None, command);

        log.debug(response)

        if status == 200:
            log.info('Creating new app {} in dataverse {}'.format(appName, appDataverse))
            apiKey = str(hashlib.sha224((appDataverse+ appName + str(datetime.now())).encode()).hexdigest())

            adminPassword = str(hashlib.sha224((appName + adminUser + adminPassword).encode()).hexdigest())
            app = Application(Application.dataverseName, appName, appName, appDataverse, adminUser, adminPassword, email, apiKey)
            yield app.save()

            # Setting up broker for this application, creating broker datasets
            result = yield self._setupBrokerForApp(appDataverse, appName)

            if result['status'] == 'failed':
                return result

            # Setting up application dataverse, creating broker datasets
            if setupAQL:
                log.debug(setupAQL)
                response = yield self.updateApplication(appName=appName, apiKey=apiKey, setupAQL=setupAQL)
                return response
            else:
                return {
                    'status': 'success',
                    'appDataverse': appDataverse,
                    'appName': appName,
                    'apiKey': apiKey
                }
        else:
            return {
                'status': 'failed',
                'error': response
            }

    @tornado.gen.coroutine
    def checkApplication(self, appName, apiKey):
        applications = yield Application.load(dataverseName=Application.dataverseName, appName=appName)

        if not applications or len(applications) == 0:
            log.error('No application `%s` exists' % appName)
            return {
                'status': 'failed',
                'error': 'No application exists'
            }
        elif applications[0].apiKey != apiKey:
            log.error('API key does not match')
            return {
                'status': 'failed',
                'error': 'Application APIKey does not match'
            }
        return {'status': 'success'}, applications[0]

    @tornado.gen.coroutine
    def updateApplication(self, appName, apiKey, setupAQL=None):
        # Check if an application already exists, if so match ApiKey
        check, app = yield self.checkApplication(appName, apiKey)
        if check['status'] == 'failed':
            return check

        if setupAQL is None or len(setupAQL) == 0:
            return {
                'status': 'failed',
                'error': 'No setup AQL is provided.'
            }

        dataverseName = app.appDataverse
        log.info('Setting up or updating dataverse {} for app {}....'.format(dataverseName, appName))

        # The setup AQL MUST not contain use dataverse or create dataverse commands
        if 'use ' in setupAQL or 'create dataverse' in setupAQL:
            return {
                'status': 'failed',
                'error': 'The AQL command MUST not contain `use dataverse` or `create dataverse` commands'
            }

        status, response = yield self.asterix.executeSQLPP(dataverseName, setupAQL)
        log.debug(response)

        if status == 200:
            log.info('Setup for app %s is successful' %appName)
            return {
                'status': 'success',
                'appName': appName,
                'apiKey': apiKey,
            }
        else:
            log.info('Setup for app `%s` is failed' %appName)
            return {
                'status': 'failed',
                'appName': appName,
                'error': 'Setup failed, possible reason %s' % response
            }

    @tornado.gen.coroutine
    def applicationAdminLogin(self, appName, adminUser, adminPassword):
        applications = yield Application.load(dataverseName=Application.dataverseName, appName=appName)

        if not applications or len(applications) == 0:
            return {
                'status': 'failed',
                'error': 'No application exists with name `%s`' % appName
            }

        app = applications[0]
        adminPassword = str(hashlib.sha224((appName + adminUser + adminPassword).encode()).hexdigest())

        if (adminUser, adminPassword) == (app.adminUser, app.adminPassword):
            log.info('App admin `%s` logs in' % adminUser)
            return {
                'status': 'success',
                'appName': appName,
                'adminUser': adminUser,
                'apiKey': app.apiKey
            }
        else:
            log.error('Password does not match')
            return {
                'status': 'failed',
                'error': 'No application exists or Password does not match'
            }

    @tornado.gen.coroutine
    def adminQueryListChannels(self, appName, apiKey):
        # Check if application exists, if so match ApiKey
        check, app = self.checkApplication(appName, apiKey)
        if check['status'] == 'failed':
            return check

        dataverseName = app.appDataverse

        aql = 'SELECT * FROM Channel WHERE DataverseName = \"{}\"'.format(dataverseName)
        status, response = yield self.asterix.executeSQLPP('Metadata', aql)

        log.debug(response)
        if status == 200 and response:
            return {
                'status': 'success',
                'channels': json.loads(str(response, 'utf-8'))
            }
        else:
            return {
                'status': 'failed',
                'error': 'No channel exists in the app dataverse'
            }

    @tornado.gen.coroutine
    def adminQueryListSubscriptions(self, appName, apiKey, channelName):
        # Check if application exists, if so match ApiKey
        check, app = self.checkApplication(appName, apiKey)
        if check['status'] == 'failed':
            return check

        dataverseName = app.appDataverse
        subscriptions = yield UserSubscription.load(dataverseName, channelName=channelName)

        log.debug(subscriptions)
        if subscriptions and len(subscriptions) > 0:
            return {
                'status': 'success',
                'subscriptions': subscriptions
            }
        else:
            return {
                'status': 'failed',
                'error': 'No subscription exists in this channel'
            }

    @tornado.gen.coroutine
    def brokerToBrokerCall(self, calldata):
        log.info('Got brokercall with this data ' + str(calldata))
        authKey = calldata['authKey']
        command = calldata['command']
        data = calldata['data']

        if command == 'userchangedbroker':
            dataverseName = data['dataverseName']
            userId = data['userId']
            self.clearStateForUser(dataverseName, userId)

    @tornado.gen.coroutine
    def _setupBrokerForApp(self, dataverseName, appName):
        log.info('Setting up broker datasets for this dataverse...')
        commands = ''
        '''
        with open("brokersetupforapp.aql") as f:
            for line in f.readlines():
                if not line.startswith('#'):
                    commands = commands + line
        commands = commands + '\n'
        '''

        for cls in [User, ChannelSubscription, UserSubscription]:
            commands += cls.getCreateStatement();

        #commands = commands + 'create broker {} at "http://{}:{}/notifybroker"'.format(self.brokerName, self.brokerIPAddr, self.brokerPort)

        status, response = yield self.asterix.executeSQLPP(dataverseName, commands)

        if status == 200:
            log.info('Broker setup succeeded for app {}'.format(appName))
            return {
                'status': 'success',
            }
        else:
            log.error('Broker setup failed ' + response)
            return {'status': 'failed', 'error': response}


def set_live_web_sockets(web_socket_object):
    global live_web_sockets
    mutex.acquire()
    try:
        live_web_sockets.add(web_socket_object)
    finally:
        mutex.release()

if __name__ == '__main__':
    broker = BADBroker.getInstance()
    #tornado.ioloop.IOLoop.current().add_callback(broker.dropChannelResults())
    tornado.ioloop.IOLoop.current().add_callback(broker.adminQueryListChannels('demoapp'))
    tornado.ioloop.IOLoop.current().start()
