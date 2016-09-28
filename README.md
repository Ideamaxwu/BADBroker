# README #

* The broker is an HTTP server that hosts a set of REST calls.

* All calls are POST calls to a URI relative to the broker's URL. Calls are made from clients except for "notifybroker", which is called from the Asterix backend. Request and response body (JSON objects) of the calls are shown below. Responses are shown for successful calls. For failure, "status" is set to "failed" and a corresponding "error" message is returned. 
 
* The broker (by default) listens at port 8989.

* The usual flow of calls from a client program to the broker is as follows. The client program registers to the broker by making "register", followed by "login". Once logged in, the client invokes "subscribe" in order to subscribe to the designated channel and "wait" for receiving notifications from the broker (indicating that new results are available in the subscribed channel). After receiving the notification, the client makes "getresults" call to retrieve the results. 

* Actually, there is an another call to locate the broker, to begin with. This is supported by a Broker Coordination Service (BCS) that manages the network of brokers. The client talks to the BCS to get the broker URL first and then makes all the above mentioned calls to the broker.


### Run the broker ###
* Start by `python3 BADWebserver.py` located at directory `badbroker`. The broker is based on `tornado` framework in python3. 

## register ##
```
{ 
	"dataverseName": string,
	"userName" : string, 
	"email" : string,  
	"password" : string, 
} 
```

Response:
```
{ 
	"status": "success", 
	"userId" : string 
} 
```

* Example call: http://<brokerIp>:8989/register
 
## login ##
```
{ 
	"dataverseName": string,
	"userName" : string, 
	"password" : string, 
	"platform": string [possible values: "desktop", "web", "android"],
	"gcmRegistrationToken": string [for "android" platform]
} 
```
Response:
```
{ 
	"status": "success", 
	"userId" : string, 
	"accessToken": string 
} 
```
 
* UserId and accessToken received in a successful login call are used in all successive calls. So, the user needs to store them.
 
## logout ##
```
{ 
	"dataverseName": string,
	"userId" : string, 
	"accessToken" : string 
} 
```

Response:
```
{ 
	"status": "success", 
} 
```

## subscribe ##

* Subscribe to a parametermized channel by passing channel name and its parameter values.
``` 
{ 
	"dataverseName": string,
	"userId" : string, 
	"appId" : string, 
	"accessToken": string, 
	"channelName" : string,  
	"parameters" : [param1, param2, ...] (i.e., JSON array)
} 
```
Response:
```
{ 
	"status": "success", 
	"userSubscriptionId": string, 
	"timestamp": string  
} 
```

## unsubscribe ##
* Unsubscribe from a subscription user made earlier
```
{
	"dataverseName": string,
	"userId": string,
	"accessToken": string,
	"userSubscriptionId": string
} 
```

## getresults ##
* Fetch results from a subscribed channel. This call is assumed to be made in response to a notification received from the broker that indicates that new results are populated for the channel and are ready to be fetched.
```
{
	"dataverseName": string,
	"userId": string,
	"accessToken": string,
	"channelName": string,
	"userSubscriptionId": string,
	"channelExecutionTime": string
}
```
Response:
```
{
	"status": "success",
	"results": [... JSON Array...]
}
```

## listchannels ## 
* List of all available channels in the system 
```
{
	"dataverseName": string,
	"userId" : string, 
	"accessToken" : string 
} 
```
Response:
```
{ 
	"status": "success", 
	"channels" : [ object (channel) ] 
} 
```
## listsubscriptions ## 
* List all subscriptions of a user
``` 
{ 
	"dataverseName": string,
	"userId" : string, 
	"accessToken" : string 
} 
```
Response:
```
{ 
	"status": "success", 
	"subscriptions" : [ 
	{  
		"channel": string,  
		"subscriptionId" : string,  
		"parameters" : [value1, value2, ...] 
	} 
	] 
} 
```  
## gcmregistration ## 
* Send the Android client's GCM registration token to the broker if the token is refreshed.
```
{
	"dataverseName": string,
	"userId": string,
	"accessToken": string,
	"gcmRegistrationToken": string
} 
```
Response:
```
{
	"status": "success"
}
```

## notifybroker ##

* This is invoked by the Asterix Backend
``` 
{ 
	"dataverseName" : string, 
	"channelName" : string, 
	"channelExecutionTime": string,
	"subscriptionIds": [string]
} 
```
Response:
```
{ 
	"status": "success"
}
```