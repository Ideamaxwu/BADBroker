app.controller('PreferenceController', ['$scope', '$window', '$filter', 'PreferenceGetter', 'SessionStorage', 
	function($scope, $window, $filter, PreferenceGetter, SessionStorage) { 

		$scope.accessToken = SessionStorage.get('accessToken');
		$scope.userId = SessionStorage.get('userId');
		$scope.chkbxs = [];

	var successFunction = function(data) {
		console.log("SAFIR-->All is well with subscriptions!");

		SessionStorage.set('subscriptionId', data['data']['userSubscriptionId']);
		SessionStorage.set('timestamp', data['data']['timestamp']);

		$window.location.href = '/notifications';
	}

	var errorFunction = function(data) {
		console.log("Something went wrong: " + data);
	}

	var channelListSuccessFuction = function(data) {
		console.log("PURVI--> channelListSuccessFuction");
		console.log("This the returned data  test3:"+ data['data']["channels"][0]["ChannelName"]);
		var i=0;
		for(obj in data['data']["channels"])
		{
			console.log("The "+ i +"data "+data['data']["channels"][i]["ChannelName"]);
			$scope.chkbxs.push({channel:data['data']["channels"][i]["ChannelName"],val:false,label:''});
			i++;
		}
	
	}


	$scope.getChannels = function(){
		console.log("In getChannels()");
		$scope.isActive = true;
		$scope.accessToken = SessionStorage.get('accessToken');
		$scope.userId = SessionStorage.get('userId');
		PreferenceGetter.getChannelList($scope.userId, $scope.accessToken, channelListSuccessFuction,errorFunction);
	};

	$scope.subscribeToNotifications = function() {
		$scope.isActive = true;
		var subscriptionList = $filter("filter")( $scope.chkbxs , {val:true} );
		for (var i = 0; i < subscriptionList.length; i++) {
			subscriptionList[i] = subscriptionList[i].label;
		}
		console.log(subscriptionList);
		$scope.accessToken = SessionStorage.get('accessToken');
		$scope.userId = SessionStorage.get('userId');

		PreferenceGetter.postSubscription($scope.userId, $scope.accessToken, subscriptionList, 
			successFunction, errorFunction);

	};

}]);