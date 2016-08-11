app.factory('SessionStorage', function($window) {
	function set(key, data) {

		if(key == 'subscriptionId' || key == 'timestamp') {
			if($window.sessionStorage.getItem(key)) {
				var keyList = JSON.parse($window.sessionStorage.getItem(key));
				console.log(keyList);
				keyList.push(data);
				$window.sessionStorage.setItem(key, JSON.stringify(keyList));
			}
			else {
				var keyList = [data];
				$window.sessionStorage.setItem(key, JSON.stringify(keyList));
			}
		}
		else {
			$window.sessionStorage.setItem(key, data)	
		}
		
		console.log($window.sessionStorage.getItem(key));
	}

	function get(key) {
		return $window.sessionStorage.getItem(key);
	}

	function remove() {
		$window.sessionStorage.clear();
	}

	function removeElement(key) {
		if($window.sessionStorage.getItem(key) != null) {
			$window.sessionStorage.removeItem(key);
		}
		else {
			console.log("Key is not present in local storage: " + key);
		}
	}

	return {
		set : set,
		get : get,
		remove : remove, 
		removeElement : removeElement
	}

});