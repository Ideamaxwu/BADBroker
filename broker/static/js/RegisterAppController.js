app.controller('RegisterAppController',['$scope','RegisterAppGetter',function($scope,RegisterAppGetter){
	SessionStorage.conf();
	var successFunction = function(data) {
		console.log("1deamxwu ---> register application respond success");
		if(data['data']['status']=='success'){
			console.log('1deamxwu ---> register application Success as '+data['data']['appName']);
			$window.location.href = '/index.html';
		}else{
			console.log("1deamaxwu ---> register application ERROR: "+data['data']['error'])	
			$window.alert(data['data']['error']);	
		}

};
	var errorFunction = function(data) {
		console.log("1deamxwu ---> respond ERROR: " + data['data']);
		$window.alert(data['data']);
};
	$scope.registerUser = function(newAppName, newAppDataverse, newAdminUser, newAdminPassword, newEmail, dropExisting, newSetupAQL){
		RegisterAppGetter.postRegisterAppData(newAppName, newAppDataverse, newAdminUser, newAdminPassword, newEmail, dropExisting, newSetupAQL, SessionStorage.get('brokerUrl'), successFunction, errorFunction);
};
}]);
