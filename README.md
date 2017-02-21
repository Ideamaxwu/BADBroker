# README #

* The broker is an HTTP server, which hosts a set of REST calls.

* All calls are POST calls to URIs relative to the broker's URL. All calls are made from client programs except  "notifybroker", which is called from the Asterix backend. Request and response body are JSON objects and their formats are shown below. All response bodies contain a 'status' field indicating the status of the call. When a call is failed, "status" is set to "failed" (otherwise set of "success") and a corresponding "error" message is returned. 
 
* The broker (by default) listens at port 8989.

* The usual flow of calls from a client program to the broker is as follows. The client program registers to the broker by invoking "register" call, followed by "login". Once logged in, the client invokes "subscribe" in order to subscribe to the designated channel and then "wait" for receiving notifications from the broker (indicating that new results are available in the subscribed channel). After receiving the notification, the client makes "getresults" to retrieve the results. 

* All broker nodes belong to a broker network, which is managed by the Broker Coordination Service (BCS). The client talks to the BCS to get the broker URL first (if not known apriori) and then makes all the above mentioned calls to the broker.


### Run the broker ###
* First, you need to setup the Asterix backend (install AsterixDB from a BAD enabled asterix branch). 

* Once, the Asterix backend is ready, start the broker by `python3 BADWebserver.py` located at directory `broker`. The broker is based on `tornado` framework in Python 3.

* **NOTE**: If you're an application developer, you need to register your application through the broker. Hit the base broker URL (e.g., localhost:8989 if the broker is installed locally, otherwise use the appropriate broker server). You will be shown a webpage with the application registration option. Follow the link to provide the application name, dataverse name (currently application name and dataverse name need to be the same), and other information as well as your application setup AQL script (to be pasted into the text area in the page) to create the necessary datasets and channels. On success, the  registration returns an API key that can be later used for managing the application (*this feature is not currently implemented though*).

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
* Fetch results from a subscribed channel. This call is assumed to be made in response to a notification received from the broker that indicates that new results are populated for the subscription and are ready to be fetched.
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

## callfunction ##
* Return results from a function call.

```
{
	"dataverseName": string,
	"userId": string,
	"accessToken": string,
	"functionName": string,
	"parameters": [param1, param2, ...] (i.e.,JSON array)	
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

## notification from the broker ##

```
{
   "userId": string,
   "dataverseName": string,
   "channelName":  string,   
   "userSubscriptionId": string,
   "channelExecutionTime": string
}
```