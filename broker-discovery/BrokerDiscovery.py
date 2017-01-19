import maxminddb
from math import radians, cos, sin, asin, sqrt
import sys

class Broker(object):
    def __init__(self,brokerName,brokerIP,brokerLatitude,brokerLongitude):
        self.Name=brokerName
        self.IP=brokerIP
        self.Latitude=brokerLatitude
        self.Longitude=brokerLongitude

class IPtoLatLong(object):
    def __init__(self,GeoIPdatabaseName='GeoLite2-City.mmdb'):
        try:
            self.reader = maxminddb.open_database(GeoIPdatabaseName)
        except:
            print("Maxmind IP to Lat Long database not present in path.")
        self.brokers=[]

    def getLatLong(self,ipAddress):
        try:
            return (self.reader.get(ipAddress)['location']['latitude'],self.reader.get(ipAddress)['location']['longitude'])
        except:
            return (None,None)

    def registerBroker(self,brokerName,brokerIP):
        (brokerLatitude,brokerLongitude)=self.getLatLong(brokerIP)
        self.brokers.append(Broker(brokerName,brokerIP,brokerLatitude,brokerLongitude))
        print ("Added a broker")

    def getNearestBroker(self,clientIP):
        minDistance=sys.maxsize
        (clientLatitude,clientLongitude)=self.getLatLong(clientIP)
        if not (self.brokers):
            return None
        if (not clientLatitude or not clientLongitude):
            return self.brokers[0]
        nearestBroker=self.brokers[0]
        for broker in self.brokers:
            distanceToCurrentBroker=self.haversine(clientLongitude,clientLatitude,broker.Longitude,broker.Latitude)
            if (distanceToCurrentBroker<minDistance):
                minDistance=distanceToCurrentBroker
                nearestBroker=broker
        return nearestBroker

    def haversine(self,lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # Flicked from a Stack Overflow answer at http://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 6371 # Radius of earth in kilometers. Use 3956 for miles
        return c * r

if __name__=="__main__":
    reg=IPtoLatLong()
    reg.registerBroker('abc','12.23.44.11')
    print(reg.getNearestBroker('127.0.0.0').IP)
