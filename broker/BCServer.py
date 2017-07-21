import os
import tornado.ioloop
import tornado.web
import logging as log
import simplejson as json
from threading import Lock, Timer
from brokerobjects import *

log = brokerutils.setup_logging(__name__)

class BCServer:
    BCSInstance = None
    
    @classmethod
    def getInstance(cls):
        if BCServer.BCSInstance is None:
            BCServer.BCSInstance = BCServer()
        return BCServer.BCSInstance
    def __init__(self):
        log.info("BCServer start.")
        self.brokers = {}
    
    def BrokersCheck(self):
        Timer(60*10, self.BrokersCheck).start()
        log.info('1deamaxwu ==================> BCS Brokers Status <===================')
        if len(self.brokers) == 0:
            log.info("> NO broker is ACTIVE.")
        else:
            for broker in list(self.brokers):
                log.info("> " + broker + " is ACTIVE.")
    
class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "Content-Type")
        self.set_header('Access-Control-Allow-Methods', "GET, POST, OPTIONS")
	
    def get(self):
        log.info("get")
    def post(self):
        log.info("post")
    def options(self):
        log.info("options")
             
class RegisterBrokerHandler(BaseHandler):
    def initialize(self, bcs):
        self.bcs = bcs
        
    def get(self):
        print(self.request.body)
        
    @tornado.gen.coroutine
    def post(self):
        post_data = json.loads(str(self.request.body, encoding='utf-8'))
        try:
            brokerName = post_data['brokerName']
            brokerIP = post_data['brokerIP']
            brokerPort = post_data['brokerPort']
            
            log.info("brokerName " + brokerName + ", brokerIP: " + brokerIP + ", brokerPort: " + brokerPort)
            
            bcs.brokers[brokerName] = brokerIP + ":" + brokerPort
            log.info(bcs.brokers)

            response={'status':'success'}
        
        except Exception as e:
            response={'status':'failed','error':str(e)}
            
        self.write(json.dumps(response))
        self.flush()
        self.finish()

class GetBrokerHandler(BaseHandler):
    def initialize(self, bcs):
        self.bcs = bcs
        
    def get(self):
        print(self.request.body)
    
    
    def brokerSelector(self, brokerDict):
        for broker in brokerDict:
            return broker, brokerDict[broker]
        return 'brokerUrl', 'radon.ics.uci.edu:9110'
        
    @tornado.gen.coroutine
    def post(self):
        post_data = json.loads(str(self.request.body, encoding='utf-8'))
        try:
            platform = post_data['platform']
            
            log.info("platform " + platform)
            
            brokerName, brokerUrl = self.brokerSelector(self.bcs.brokers)

            response={'status':'success', 'brokerUrl': brokerUrl}
        
        except Exception as e:
            response={'status':'failed','error':str(e)}
            
        self.write(json.dumps(response))

        self.flush()
        self.finish()

def make_app(bcs):
    return tornado.web.Application([
        (r"/registerbroker", RegisterBrokerHandler, dict(bcs=bcs)),
        (r"/getbroker", GetBrokerHandler, dict(bcs=bcs)),
    ])

if __name__ == "__main__":
    bcs = BCServer.getInstance()
    bcs.BrokersCheck()
    app = make_app(bcs)
    app.listen(5000)
    tornado.ioloop.IOLoop.current().start()
