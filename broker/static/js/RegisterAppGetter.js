app.factory('RegisterAppGetter',['$http',function($http){
	return{
		postRegisterAppData: function(newAppName, newAppDataverse, newAdminUser, newAdminPassword, newEmail, dropExisting, newSetupAQL, successFunction, errorFunction){
			console.log("1deamxwu ---> registering application: "+newAppName+'\t'+newAppDataverse+'\t'+newAdminUser+'\t'+newAdminPassword+'\t'+newEmail+ '\t'+dropExisting+'\t'+newSetupAQL);
			var message={
				'appName':newAppName,
				'appDataverse':newAppDataverse,
				'adminUser':newAdminUser,
				'adminPassword':newAdminPassword,
				'email':newEmail,
				'dropExisting':dropExisting,
				'setupAQL':newSetupAQL

};
			$http({
				url: 'http://'+url+'/registerapplication',
				method:'POST',
				data:message,
			}).then(successFunction, errorFunction);
}
};
}]);
