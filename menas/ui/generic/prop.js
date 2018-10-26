/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Utility for (null/undefined)-safe handling of JSON objects...
 */

var Prop = new function() {
	/**
	 * Given an object and a dot-separated path in the object, safely return the value or undefined
	 * 
	 * Example:
	 * Prop.get(myObj, "a.b.c.d")
	 * 
	 * If the object has {a:{b:{c:{d:x}}}}, returns x
	 * If the object is {a:x}, returns safely undefined
	 * 
	 */	
	this.get = function(oObj, sProp){
		if(!oObj || !sProp) return;
		var curr = oObj;
		var parts = sProp.split(".");
		for(var i =0; i < parts.length; i++) {
			curr = curr[parts[i]];
			if(!curr) return;
		}
		return curr;
	};
	
	/**
	 * Similarly to Prop.get, this safely sets the value to a property in an object
	 */
	this.set = function(oObj, sProp, val){
		if(!oObj || !sProp) return;
		var curr = oObj;
		var parts = sProp.split(".");
		for(var i =0; i < parts.length; i++) {
			if(i == parts.length-1) curr[parts[i]] = val; 
			else if(!curr[parts[i]]) curr[parts[i]] = {};
			curr = curr[parts[i]];
		}
	};
	
	/**
	 * Converts an object to an array (omitting the keys)
	 */
	this.objToArr = function(obj) {
		var res = [];
		for(var i in obj) {
			res.push(obj[i]);
		}
		return res;
	}
	
	
}();