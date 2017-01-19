from BrokerDiscovery import IPtoLatLong,Broker
from flask import Flask, jsonify, redirect
from flask import request
app = Flask(__name__)

@app.route("/")
def getNearestBroker():
    global geoIP
    clientIP=str(request.environ.get('HTTP_X_REAL_IP', request.remote_addr))
    broker=geoIP.getNearestBroker(clientIP)
    if broker:
        broker_address = 'http://' + broker.IP + ':8989'

        return redirect(broker_address, 302)
    else:
        return "Error. No brokers registered."

    #return "Hello from the other side"


@app.route("/registerbroker", methods=['POST'])
def registerBroker():
    global geoIP
    try:
        result = request.get_json()
        brokerName = result['brokerName']
        brokerIP = result['brokerIP']
        geoIP.registerBroker(brokerName,brokerIP)
        return "Added broker successfully."
    except:
        return "Badly formatted request. Keys must be brokerName and brokerIP"


@app.route("/getnearestbroker")
def getBrokerIPAddress():
    global geoIP
    clientIP=str(request.environ.get('HTTP_X_REAL_IP', request.remote_addr))
    broker=geoIP.getNearestBroker(clientIP)
    if broker:
        return jsonify({'brokerIP':broker.IP})
    else:
        return "Error. No brokers registered."


@app.route("/ip")
def getIP():
    global geoIP
    ipAddress=str(request.environ.get('HTTP_X_REAL_IP', request.remote_addr))
    (latitude,longitude)=geoIP.getLatLong(ipAddress)
    return jsonify({'ip': request.remote_addr,'latitude':latitude,'longitude':longitude}), 200



if __name__ == "__main__":
    geoIP=IPtoLatLong()
    app.run(debug=True)
