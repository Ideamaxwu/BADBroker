import os
import tornado.ioloop
import tornado.web
import logging as log
import simplejson as json
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
    @tornado.gen.coroutine
    def post(self):
        post_data = json.loads(str(self.request.body, encoding='utf-8'))
        try:
            brokerName = post_data['brokerName']
            brokerIP = post_data['brokerIP']
            
            log.info("brokerName " + brokerName + ", brokerIP: " + brokerIP)

            response={'status':'success'}
        
        except Exception as e:
            response={'status':'failed','error':str(e)}
            
        self.write(json.dumps(response))
        self.flush()
        self.finish()

def make_app():
    return tornado.web.Application([
        (r"/registerbroker", RegisterBrokerHandler)
    ])

if __name__ == "__main__":
    bcs = BCServer.getInstance()
    app = make_app()
    app.listen(5000)
    tornado.ioloop.IOLoop.current().start()
