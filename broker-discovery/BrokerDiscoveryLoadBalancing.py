import Lock
mutex = Lock()

MAX_CONNECTIONS = 20

class Broker(object):
    def __init__(self,brokerName,brokerIP):
        self.Name=brokerName
        self.IP=brokerIP
        self.num_connections = 0

class BrokerDiscoveryLoadBalancing(object):
    def __init__(self):
        self.connection_map = {}
        self.brokers = []

    def register_broker(self, brokerName, brokerIP):
        self.brokers.append(Broker(brokerName, brokerIP))
        self.connection_map[brokerIP] = 0

    def insert_connection(self, brokerIP):
        mutex.acquire()
        try:
            self.connection_map[brokerIP] += 1
        except:
            self.connection_map[brokerIP] = 1
        mutex.release()

    def remove_connection(self, brokerIP):
        mutex.acquire()
        try:
            self.connection_map[brokerIP] -= 1
        except:
            print "Broker does not exist. Something went wrong"
        mutex.release()

    def get_least_connected_broker(self):
        pass
