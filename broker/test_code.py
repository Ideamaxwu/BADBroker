#!/usr/bin/env python3
import simplejson as json
import sys
import requests
import tornado.httpclient
import tornado.httputil
import tornado.ioloop
import tornado.gen

class BADObject:
    userName = 'abc'
    age = '12'
    password = 'abc'

    def __init__(self, objectId):
        self.objectId = objectId
    
    def load(self):
        cmd_stmt = "for $t in dataset " + str(self.__class__.__name__) + "Dataset "
        cmd_stmt = cmd_stmt + " where $t.objectId = " + str(self.objectId) + " return $t"
        print(cmd_stmt)
        
    def save(self):        
        cmd_stmt = "upsert into dataset " + self.__class__.__name__ + "Dataset"
        cmd_stmt = cmd_stmt + "("
        cmd_stmt = cmd_stmt + json.dumps(self.__dict__)
        cmd_stmt = cmd_stmt + ")"
        print(cmd_stmt)

    @classmethod
    def getCreateStatement(cls):
        statement = 'create type %sType as closed {' % (cls.__name__)
        dataitems = None
        for key, value in BADObject.__dict__.items():
            if key.startswith('__') or callable(value) or isinstance(value, classmethod):
                continue
            if type(value) is str:
                item = '%s: %s' % (key, 'string')
            else:
                raise Exception('Invalid data type for %s %s %s' % (key, value))

            dataitems = (dataitems + (',\n' + item)) if dataitems else ('\n' + item)

        statement += dataitems + '\n}\n'
        statement += 'create dataset %sDataset (%sType) primary key recordId\n' % (cls.__name__, cls.__name__);
        return statement


class User(BADObject):
    def __init__(self, userId):
        BADObject.__init__(self, userId)
        self.username = 'wewew'
        self.age = 2323
        self.password = 'fdffdffr'
    
        
def test():
    yield [1, 2]
    yield [3, 4]
    yield [4, 6]

for x in test():
    print(x)

print(User.getCreateStatement())

#user = User(3245566)
#user.load()
#user.save()

x = {'a' : 1, 'b': 2, 'c': {'t1': 1, 't2': 12}}
try:
    v = x['b']
except KeyError as kerr:
    print('error', kerr)

print(v)

for item in x:
    print (item)

for item in sorted(x.keys(), reverse=True):
    print (item)

for k, v in x.items():
    print (k, v)


class User():
    def __init__(self):
        self.__dict__ = {'userName': 'abc', 'age': 12}


u = User()

print(u.userName, u.age)


url = 'http://promethium.ics.uci.edu:19002/query'
params={'query': 'use dataverse BrokerMetadata;'}
#response = requests.get(url=url, params=params)
#print(response.status_code, response.text)

@tornado.gen.coroutine
def call():
    client = tornado.httpclient.AsyncHTTPClient()
    request = tornado.httpclient.HTTPRequest(tornado.httputil.url_concat(url, params), method='GET')
    response = yield client.fetch(request)
    print(response.headers)
    print(response.body)

    client.close()

tornado.ioloop.IOLoop.instance().add_callback(call)
tornado.ioloop.IOLoop.instance().start()

sys.exit(0)





r = redis.StrictRedis(host='localhost', port=6379, db=0)

y = {'b': 2, 'c': {'x': 10, 'z': 12, 'y': 11}, 'a': 1}
json.dumps(y)

r.set('mydict', json.dumps(y))

mydict = r.get('mydict')
print(mydict)

mydict = json.loads(str(mydict, encoding='utf-8'))
print(mydict)

print(r.exists('mydict'))
r.publish('first', 'Hello')