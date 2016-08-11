app.controller('IndexController', ['$scope', '$window', 'IndexGetter', 'SessionStorage', function($scope, $window, 
    IndexGetter, SessionStorage) { 
    var successFunction = function(data) {
        console.log("SAFIR-->All is well!");
        console.log(data['data']['accessToken']);
        SessionStorage.set('accessToken', data['data']['accessToken']);
        SessionStorage.set('userId', data['data']['userId']);
        $window.location.href = '/notifications';
    }

    var subscribeSuccessFunction = function(data) {
        console.log("SAFIR-->All is well!");
        console.log(data['data']['accessToken']);
        SessionStorage.set('accessToken', data['data']['accessToken']);
        $window.location.href = '/subscriptions';
        // $window.location.href = '/preferences';

    }

    var registerSuccessFunction = function(data) {
        console.log("SAFIR-->All is well with registration!");
        SessionStorage.set('userId', data['data']['userId']);
        $scope.loginUser($scope.newUserName, $scope.newUserPassword, false);
    }

    var errorFunction = function(data) {
        console.log("Something went wrong: " + data);
    }

    $scope.userId = '';
    $scope.userPassword = '';

    $scope.loginUser = function(userId, userPassword, fromHtml) {
        $scope.isActive = true;
        $scope.userId = userId;
        $scope.userPassword = userPassword;
        if(fromHtml) {
            console.log('It works');
            IndexGetter.postUserData($scope.userId, $scope.userPassword, successFunction, errorFunction);   
        }
        else {
            IndexGetter.postUserData($scope.userId, $scope.userPassword, subscribeSuccessFunction, errorFunction);
        }
        
    };

    $scope.registerUser = function(newUserName, newUserPassword, newUserEmail) {
        $scope.newUserName = newUserName;
        $scope.newUserPassword = newUserPassword;
        $scope.newUserEmail = newUserEmail;
        IndexGetter.postRegisterUser($scope.newUserName, $scope.newUserPassword, $scope.newUserEmail, 
            registerSuccessFunction, errorFunction);
    }
}]);