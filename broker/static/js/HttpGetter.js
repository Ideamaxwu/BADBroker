app.factory('HttpGetter', ['$http', function($http){
  return {
      pollBroker: function(successFunction, errorFunction){
        console.log("Hi");

        var message = {
          'userId' : 'abcd',
        };
        $http({
          url: '/events',
          method: "POST",
          data: message,
        }).then(successFunction, errorFunction);
      },

      getNewResults: function(userId, accessToken, subscriptionId, deliveryTime, channelName, 
        successFunction, errorFunction) {
        console.log('In Get new results');

        var message = {
          'dataverseName' : "channels",
          'userId' : userId,
          'accessToken' : accessToken,
          'channelName' : channelName,
          'userSubscriptionId' : subscriptionId,
          'deliveryTime' : deliveryTime
        };

        $http({
          url: '/getresults',
          method: "POST",
          data: message,
        }).then(successFunction, errorFunction);
      },

      getSubscriptions: function(userId, accessToken, successFunction, errorFunction) {
        console.log('In get subscriptions');

        var message = {
          'dataverseName' : "channels",
          'userId' : userId,
          'accessToken' : accessToken
        }

        $http({
          url: '/listsubscriptions',
          method: "POST",
          data: message,
        }).then(successFunction, errorFunction);
      }
    };
}]);