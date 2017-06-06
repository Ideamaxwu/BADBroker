import tornado.gen
import tornado.ioloop
import tornado.iostream
import hashlib

from datetime import datetime
import simplejson as json
from asterixapi import *
import brokerutils

log = brokerutils.setup_logging(__name__)


class Session:
    def __init__(self, dataverseName, userId, userType, accessToken, platform, creationTime, lastAccessTime):
        self.dataverseName = dataverseName
        self.userId = userId
        self.userType = userType
        self.accessToken = accessToken
        self.platform = platform
        self.user = user
        self.creationTime = creationTime
        self.lastAccessTime = lastAccessTime


class BrokerObject:
    @classmethod
    def getCreateStatement(cls):
        statement = 'CREATE TYPE %sType as closed {' % (cls.__name__)
        dataitems = None
        for key, value in cls.__dict__.items():
            if key.startswith('__') or callable(value) or isinstance(value, classmethod):
                continue
            if type(value) is str:
                item = '%s: %s' % (key, 'string')
            else:
                raise Exception('Invalid data type for %s %s %s' % (key, value))

            dataitems = (dataitems + (',\n' + item)) if dataitems else ('\n' + item)

        statement += dataitems + '\n}\n'
        statement += 'CREATE DATASET %sDataset (%sType) PRIMARY KEY recordId;\n' % (cls.__name__, cls.__name__);
        log.debug(statement)
        return statement

    @tornado.gen.coroutine
    def delete(self):
        asterix = AsterixQueryManager.getInstance()
        cmd_stmt = 'DELETE FROM ' + str(self.__class__.__name__) + 'Dataset '
        cmd_stmt = cmd_stmt + ' WHERE $t.recordId = \"{0}\"'.format(self.recordId)
        log.debug(cmd_stmt)

        status, response = yield asterix.executeUpdate(self.dataverseName, cmd_stmt)
        if status == 200:
            log.info('Delete succeeded')
            return True
        else:
            log.error('Delete failed. Error ' + response)
            raise Exception('Delete failed ' + response)

    @classmethod
    @tornado.gen.coroutine
    def deleteWhere(cls, dataverseName, **kwargs):
        asterix = AsterixQueryManager.getInstance()
        whereClause = None
        if kwargs:
            for key, value in kwargs.items():
                if isinstance(value, str):
                    clause = '$t.{} = \"{}\"'.format(key, value)
                else:
                    clause = '$t.{} = {}'.format(key, value)
                whereClause = whereClause + ' and ' + clause if whereClause else clause

        cmd_stmt = 'DELETE FROM ' + str(cls.__name__) + 'Dataset '
        cmd_stmt = cmd_stmt + ' WHERE {}'.format(whereClause)
        log.debug(cmd_stmt)

        status, response = yield asterix.executeUpdate(dataverseName, cmd_stmt)
        if status == 200:
            log.info('Delete succeeded')
            return True
        else:
            log.error('Delete failed. Error ' + response)
            raise Exception('Delete failed ' + response)

    @tornado.gen.coroutine
    def save(self):
        asterix = AsterixQueryManager.getInstance()
        cmd_stmt = 'UPSERT INTO ' + self.__class__.__name__ + 'Dataset'
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
    def load(cls, fields, dataverseName, objectName, **kwargs):
        asterix = AsterixQueryManager.getInstance()
        condition = None
        if kwargs:
            for key, value in kwargs.items():
                if isinstance(value, str) and key != 'parameters':
                    paramvalue = '\"{0}\"'.format(value)
                else:
                    paramvalue = value

                if condition is None:
                    condition = '{0} = {1}'.format(key, paramvalue)
                else:
                    condition = condition + ' and {0} = {1}'.format(key, paramvalue)
        else:
            log.warning('No argument is provided for load')
            return None

        dataset = objectName + 'Dataset'

        if condition:
            query = 'SELECT {} FROM {} where {}'.format(', '.join(fields), dataset, condition)
        else:
            query = 'SELECT {} FROM {}'.format(', '.join(fields), dataset)

        status, response = yield asterix.executeQuery(dataverseName, query)

        if status == 200 and response:
            response = response.replace('\n', ' ').replace(' ', '')
            log.info(response)
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
    dataverseName = 'BrokerMetadata'
    recordId = ''
    appName = ''
    appDataverse = ''
    adminUser = ''
    adminPassword = ''
    email = ''
    apiKey = ''

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
        fields = [key for key, value in cls.__dict__.items() if not (key.startswith('__') or callable(value) or isinstance(value, classmethod))]
        objects = yield BrokerObject.load(fields, dataverseName, cls.__name__, appName=appName)
        return Application.createFrom(objects)

    @classmethod
    @tornado.gen.coroutine
    def setupApplicationEnviroment(cls, asterix):
        statement = 'USE Metadata; SELECT DataverseName from `Dataverse` WHERE DataverseName=\"%s\"' % Application.dataverseName
        status, response = yield asterix.executeQuery(None, statement)
        dataverses = json.loads(response) if len(response) > 0 else []

        if status != 200 or len(dataverses) == 0:
            log.warning('Application metadata dataverse %s does not exist. Creating one' % (Application.dataverseName))
            status, response = yield asterix.executeSQLPP(None, 'create dataverse %s' % (Application.dataverseName))
            if status == 200:
                statement = Application.getCreateStatement()
                status, response = yield asterix.executeSQLPP(Application.dataverseName, statement)
                if status == 200:
                    return
            else:
                log.error('Dataverse %s creation failed!' % (Application.dataverseName))
                raise Exception ('Dataverse %s creation failed!' % (Application.dataverseName))

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
    dataverseName = ''
    recordId = ''
    userId = ''
    userName = ''
    password = ''
    email = ''
    registrationTime = ''
    lastLoginTime = ''
    lastLogoffTime = ''

    def __init__(self, dataverseName=None, recordId=None, userId=None, userName=None, password=None, email=None):
        self.dataverseName = dataverseName
        self.recordId = recordId
        self.userId = userId
        self.userName = userName
        self.password = password
        self.email = email

        self.registrationTime = str(datetime.now())
        self.lastLoginTime = str(datetime.now())
        self.lastLogoffTime = str(datetime.now())

    @classmethod
    @tornado.gen.coroutine
    def load(cls, dataverseName=None, userName=None):
        fields = [key for key, value in cls.__dict__.items() if not (key.startswith('__') or callable(value) or isinstance(value, classmethod))]
        objects = yield BrokerObject.load(fields, dataverseName, cls.__name__, userName=userName)
        return User.createFrom(objects)

    def __str__(self):
        return self.userName + ' ID ' + self.userId


class ChannelSubscription(BrokerObject):
    dataverseName = ''
    recordId = ''
    channelName = ''
    brokerName = ''
    parameters = ''
    parametersHash = ''
    channelSubscriptionId = ''
    latestChannelExecutionTime = ''

    def __init__(self, dataverseName=None, recordId=None, channelName=None, brokerName=None, parameters=None, channelSubscriptionId=None, currentDateTime=None):
        self.dataverseName = dataverseName
        self.recordId = recordId
        self.channelName = channelName
        self.brokerName = brokerName
        self.parameters = parameters
        self.parametersHash = str(hashlib.sha224((str(parameters)).encode()).hexdigest())
        self.channelSubscriptionId = channelSubscriptionId
        self.latestChannelExecutionTime = currentDateTime

    @classmethod
    @tornado.gen.coroutine
    def load(cls, dataverseName=None, channelName=None, brokerName=None, channelSubscriptionId=None, parametersHash=None):
        fields = [key for key, value in cls.__dict__.items() if not (key.startswith('__') or callable(value) or isinstance(value, classmethod))]
        if parametersHash:
            objects = yield BrokerObject.load(fields, dataverseName, cls.__name__, channelName=channelName, brokerName=brokerName, parametersHash=parametersHash)
        elif channelName and channelSubscriptionId:
            objects = yield BrokerObject.load(fields, dataverseName, cls.__name__, channelName=channelName, channelSubscriptionId=channelSubscriptionId)
        elif channelSubscriptionId:
            objects = yield BrokerObject.load(fields, dataverseName, cls.__name__, channelSubscriptionId=channelSubscriptionId)

        return ChannelSubscription.createFrom(objects)


class UserSubscription(BrokerObject):
    dataverseName = ''
    recordId = ''
    brokerName = ''
    userSubscriptionId = ''
    userId = ''
    channelSubscriptionId = ''
    channelName = ''
    parameters = ''
    parametersHash = ''
    timestamp = ''
    latestDeliveredResultTime = ''
    resultsDataset = ''

    def __init__(self, dataverseName=None, recordId=None, brokerName=None, userSubscriptionId=None, userId=None,
                 channelSubscriptionId=None, channelName=None, parameters=None, timestamp=None, resultsDataset=None):
        self.dataverseName = dataverseName
        self.recordId = recordId
        self.brokerName = brokerName
        self.userSubscriptionId = userSubscriptionId
        self.userId = userId
        self.channelSubscriptionId = channelSubscriptionId
        self.channelName = channelName
        self.parameters = parameters
        self.parametersHash = str(hashlib.sha224((str(parameters)).encode()).hexdigest())
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
    def load(cls, dataverseName=None, userId=None, userSubscriptionId=None, channelName=None, channelSubscriptionId=None):
        fields = [key for key, value in cls.__dict__.items() if not (key.startswith('__') or callable(value) or isinstance(value, classmethod))]
        if userId:
            objects = yield BrokerObject.load(fields, dataverseName, cls.__name__, userId=userId)
        elif userSubscriptionId:
            objects = yield BrokerObject.load(fields, dataverseName, cls.__name__, userSubscriptionId=userSubscriptionId)
        elif channelName:
            objects = yield BrokerObject.load(fields, dataverseName, cls.__name__, channelName=channelName)
        elif channelSubscriptionId:
            objects = yield BrokerObject.load(fields, dataverseName, cls.__name__, channelSubscriptionId=channelSubscriptionId)
        else:
            return None

        return UserSubscription.createFrom(objects)


class BADException(Exception):
    pass
