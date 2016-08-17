app.factory('PreferenceGetter', ['$http', function($http){
	return {
        getChannelList: function (userId, accessToken, successFunction, errorFunction) {
            console.log("getChannelList");
            var message = {
                'dataverseName': "channels",
                'userId': userId,
                'accessToken': accessToken
            };
            $http({
                url: '/listchannels',
                method: "POST",
                data: message
            }).then(successFunction, errorFunction);
        },
        postSubscription: function (userId, accessToken, parameters, successFunction, errorFunction) {
            console.log("postSubscription");
                var message = {
                    'dataverseName': "channels",
                    'userId': userId,
                    'accessToken': accessToken,
                    'channelName': 'nearbyTweetChannel',
                    'parameters': parameters
                };
                $http({
                    url: '/subscribe',
                    method: "POST",
                    data: message
                }).then(successFunction, errorFunction);
            
        }
   
    };
  }]);
