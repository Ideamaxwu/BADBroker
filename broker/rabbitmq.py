import pika
import logging as log
import brokerutils

log = brokerutils.setup_logging(__name__)

class RabbitMQ(object):
    def __init__(self, host='localhost'):
        self.host = host
        self.connection = None
        self.channel = None

    def sendMessage(self, userId, message):
        if self.connection is None or self.connection.is_closed:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))

        if self.channel is None or self.channel.is_closed:
            self.channel = self.connection.channel()

        try:
            self.channel.queue_declare(queue=userId)
        except pika.exceptions.ConnectionClosed as e:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=userId)

        log.info('Publishing %s' % message)
        self.channel.basic_publish(exchange='', routing_key=userId, body=message)

    def close(self):
        if self.channel:
            self.channel.close()
        if self.connection:
            self.connection.close()

    def __del__(self):
        self.close()