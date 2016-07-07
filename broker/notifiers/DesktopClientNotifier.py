import rabbitmq
import simplejson as json

class DesktopClientNotifier:
    def __init__(self):
        self.rabbitMQ = rabbitmq.RabbitMQ()

    def notify(self, userId, message):
        self.rabbitMQ.sendMessage(userId, json.dumps(message))

    def __del__(self):
        self.rabbitMQ.close()