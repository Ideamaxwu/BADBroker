import tornado.gen
import tornado.ioloop
import tornado.iostream

import simplejson as json
from asterixapi import *
import brokerutils

log = brokerutils.setup_logging(__name__)


class BrokerObject:
    @tornado.gen.coroutine
    def delete(self):
        asterix = AsterixQueryManager.getInstance()
        cmd_stmt = 'delete $t from dataset ' + str(self.__class__.__name__) + 'Dataset '
        cmd_stmt = cmd_stmt + ' where $t.recordId = \"{0}\"'.format(self.recordId)
        log.debug(cmd_stmt)

        status, response = yield asterix.executeUpdate(self.dataverseName, cmd_stmt)
        if status == 200:
            log.info('Delete succeeded')
            return True
        else:
            log.error('Delete failed. Error ' + response)
            raise Exception('Delete failed ' + response)

    @tornado.gen.coroutine
    def save(self):
        asterix = AsterixQueryManager.getInstance()
        cmd_stmt = 'upsert into dataset ' + self.__class__.__name__ + 'Dataset'
        cmd_stmt = cmd_stmt + '('
        cmd_stmt = cmd_stmt + json.dumps(self.__dict__)
        cmd_stmt = cmd_stmt + ')'
        log.debug(cmd_stmt)

        status, response = yield asterix.executeUpdate(self.dataverseName, cmd_stmt)
        if status == 200:
            log.info('Object %s Id %s saved' % (self.__class__.__name__, self.recordId))
            return True
        else:
            log.error('Object save failed, Error ' + response)
            raise Exception('Object save failed ' + response)

    @classmethod
    @tornado.gen.coroutine
    def load(cls, dataverseName, objectName, **kwargs):
        asterix = AsterixQueryManager.getInstance()
        condition = None
        if kwargs:
            for key, value in kwargs.items():
                if isinstance(value, str):
                    paramvalue = '\"{0}\"'.format(value)
                else:
                    paramvalue = value

                if condition is None:
                    condition = '$t.{0} = {1}'.format(key, paramvalue)
                else:
                    condition = condition + ' and $t.{0} = {1}'.format(key, paramvalue)
        else:
            log.warning('No argument is provided for load')
            return None

        dataset = objectName + 'Dataset'

        if condition:
            query = 'for $t in dataset {0} where {1} return $t'.format(dataset, condition)
        else:
            query = 'for $t in dataset {0} return $t'.format(dataset)

        status, response = yield asterix.executeQuery(dataverseName, query)

        if status == 200 and response:
            response = response.replace('\n', ' ').replace(' ', '')
            print(response)
            if len(response) > 0:
                return json.loads(response, encoding='utf-8')
            else:
                return None
        else:
            return None

    @classmethod
    def createFrom(cls, objects):
        if not objects:
            return None

        if isinstance(objects, list):
            instances = []
            for object in objects:
                instance = cls()
                if not object or not isinstance(object, dict):
                    log.error('Creating %s Invalid argument %s' % (cls.__name__, object))
                    return None

                instance.__dict__ = object
                instances.append(instance)
            return instances
        else:
            object = objects
            if not isinstance(object, dict):
                log.error('Creating %s Invalid argument %s' % (cls.__name__, object))
                return None

            instance = cls()
            instance.__dict__ = object
            return instance


class Application(BrokerObject):
    dataverseName = "BrokerMetadata"

    def __init__(self, dataverseName=None, recordId=None, appName=None, appDataverse=None, adminUser=None,
                 adminPassword=None, email=None, apiKey=None):
        self.dataverseName = dataverseName
        self.recordId = recordId
        self.appName = appName
        self.appDataverse = appDataverse
        self.adminUser = adminUser
        self.adminPassword = adminPassword
        self.email = email
        self.apiKey = apiKey

    @classmethod
    @tornado.gen.coroutine
    def load(cls, dataverseName=None, appName=None):
        objects = yield BrokerObject.load(dataverseName, cls.__name__, appName=appName)
        return Application.createFrom(objects)

    @classmethod
    @tornado.gen.coroutine
    def matchApiKey(self, appName, apiKey):
        applications = yield Application.load(appName=appName)

        if not applications or len(applications) == 0 or applications[0].apiKey != apiKey:
            log.error('No application or ApiKey does not match')
            return False
        else:
            return True


class User(BrokerObject):
    def __init__(self, dataverseName=None, recordId=None, userId=None, userName=None, password=None, email=None):
        self.dataverseName = dataverseName
        self.recordId = recordId
        self.userId = userId
        self.userName = userName
        self.password = password
        self.email = email

    @classmethod
    @tornado.gen.coroutine
    def load(cls, dataverseName=None, userName=None):
        objects = yield BrokerObject.load(dataverseName, cls.__name__, userName=userName)
        return User.createFrom(objects)

    def __str__(self):
        return self.userName + ' ID ' + self.userId


class ChannelSubscription(BrokerObject):
    def __init__(self, dataverseName=None, recordId=None, channelName=None, brokerName=None, parameters=None, channelSubscriptionId=None, currentDateTime=None):
        self.dataverseName = dataverseName
        self.recordId = recordId
        self.channelName = channelName
        self.brokerName = brokerName
        self.parameters = parameters
        self.channelSubscriptionId = channelSubscriptionId
        self.latestChannelExecutionTime = currentDateTime

    @classmethod
    @tornado.gen.coroutine
    def load(cls, dataverseName=None, channelName=None, brokerName=None, channelSubscriptionId=None, parameters=None):
        if parameters:
            objects = yield BrokerObject.load(dataverseName, cls.__name__, channelName=channelName, brokerName=brokerName, parameters=parameters)
        elif channelName and channelSubscriptionId:
            objects = yield BrokerObject.load(dataverseName, cls.__name__, channelName=channelName, channelSubscriptionId=channelSubscriptionId)
        elif channelSubscriptionId:
            objects = yield BrokerObject.load(dataverseName, cls.__name__, channelSubscriptionId=channelSubscriptionId)

        return ChannelSubscription.createFrom(objects)


class UserSubscription(BrokerObject):
    def __init__(self, dataverseName=None, recordId=None, userSubscriptionId=None, userId=None, channelSubscriptionId=None,
                 channelName=None, timestamp=None, resultsDataset=None):
        self.dataverseName = dataverseName
        self.recordId = recordId
        self.userSubscriptionId = userSubscriptionId
        self.userId = userId
        self.channelSubscriptionId = channelSubscriptionId
        self.channelName = channelName
        self.timestamp = timestamp
        self.latestDeliveredResultTime = timestamp
        self.resultsDataset = resultsDataset

    def __str__(self):
        return self.userSubscriptionId

    def __repr__(self):
        return self.userSubscriptionId

    def for_json(self):
        return self.__dict__

    @classmethod
    @tornado.gen.coroutine
    def load(cls, dataverseName=None, userId=None, userSubscriptionId=None):
        if userId:
            objects = yield BrokerObject.load(dataverseName, cls.__name__, userId=userId)
        elif userSubscriptionId:
            objects = yield BrokerObject.load(dataverseName, cls.__name__, userSubscriptionId=userSubscriptionId)
        else:
            return None

        return UserSubscription.createFrom(objects)


class BADException(Exception):
    pass
