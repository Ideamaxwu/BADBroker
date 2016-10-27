import tornado.ioloop
import BADBroker

broker = BADBroker.BADBroker.getInstance()
tornado.ioloop.IOLoop.current().add_callback(broker.dropChannelResults())
tornado.ioloop.IOLoop.current().start()