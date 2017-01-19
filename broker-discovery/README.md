# broker-discovery
Service that allows clients to find their closest broker. 
## Python module dependencies (Use pip to install)
flask

maxminddb
## Deployment 
`python DiscoveryServer.py `
runs it on port 5000 of localhost. 
## Usage 
Register a broker by sending a POST request to /registerbroker with parameters as brokerName and brokerIP. For example,
`http://localhost:5000/registerbroker?brokerName='BAD'&brokerIP='12.3.29.12'` regsiters a broker with name BAD and IP address 12.3.29.12.

Send a GET request to `http://localhost:5000/getnearestbroker` to get the IP address of the closest broker. The approximate location (latitude,longitude) of the client is obtained from the IP address using Maxmind's GeoIP databases and the geographically closest broker is returned.  
