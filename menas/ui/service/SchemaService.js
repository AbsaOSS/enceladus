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

jQuery.sap.require("sap.m.MessageBox");
var SchemaService = new function() {
	
	var model = sap.ui.getCore().getModel();
	
	this.getSchemaList = function(bLoadFirst, bGetAllVersionsOfFirst) {
		Functions.ajax("api/schema/list", "GET", {}, function(oData) {
			model.setProperty("/schemas", oData)
			if(oData.length > 0 && bLoadFirst) {
				SchemaService.getSchemaVersion(oData[0]._id, oData[0].latestVersion)
			} else if(bGetAllVersionsOfFirst) {
				SchemaService.getAllSchemaVersions(oData[0]._id)
			}
		}, function() {
			sap.m.MessageBox.error("Failed to get the list of schemas. Please wait a moment and try reloading the application")
		})
	};
	
	this.getLatestSchemaVersion = function(sId) {
		Functions.ajax("api/schema/detail/" + encodeURI(sId) + "/latest", "GET", {}, function(oData) {
			model.setProperty("/currentSchema", oData)
			SchemaService.getSchemaUsedIn(oData.name, oData.version)
		}, function() {
			sap.m.MessageBox.error("Failed to get the detail of the schema. Please wait a moment and try reloading the application")
			window.location.hash = "#/schema"
		})		
	};
	
	this.getSchemaVersion = function(sId, iVersion, sModelPath) {
		var modelPath;
		if(sModelPath) modelPath = sModelPath
		else modelPath = "/currentSchema"
		Functions.ajax("api/schema/detail/" + encodeURI(sId) + "/" + encodeURI(iVersion), "GET", {}, function(oData) {
			model.setProperty(modelPath, oData)
			SchemaService.getSchemaUsedIn(oData.name, oData.version)
		}, function() {
			sap.m.MessageBox.error("Failed to get the detail of the schema. Please wait a moment and try reloading the application")
			window.location.hash = "#/schema"			
		})		
	};
	
	this.updateSchema = function(sId, iVersion, sDesc) {
		Functions.ajax("api/schema/edit", "POST", {
			name: sId,
			version: iVersion,
			description: sDesc
		}, function(oData) {
			model.setProperty("/currentSchema", oData)
			SchemaService.getSchemaList();
		}, function() {
			sap.m.MessageBox.error("Failed to update the schema. Please wait a moment and try reloading the application")
		})			
	};
	
	this.getSchemaUsedIn = function(sId, iVersion) {
		Functions.ajax("api/schema/usedIn/" + encodeURI(sId) + "/" + encodeURI(iVersion), "GET", {}, function(oData) {
			model.setProperty("/currentSchema/usedIn", oData)
		}, function() {
			sap.m.MessageBox.error("Failed to retreive the 'Used In' section, please try again later.")
		})			
	};
	
	this.getAllSchemaVersions = function(sName, oControl) {
		if(oControl) oControl.setBusy(true);
		Functions.ajax("api/schema/allVersions/" + encodeURI(sName), "GET", {}, function(oData) {
			model.setProperty("/currentSchemaVersions", oData)
			if(oControl) oControl.setBusy(false);
		}, function() {
			sap.m.MessageBox.error("Failed to retreive all versions of the schema, please try again later.")
			oControl.setBusy(false);
		}, oControl)	
	};
	
	this.disableSchema = function(sId, iVersion) {
		var uri = "api/schema/disable/" + encodeURI(sId)
		if(typeof(iVersion) !== "undefined") {
			uri += "/" + encodeURI(iVersion)
		}
		
		Functions.ajax(uri , "GET", {}, function(oData) {
			if(Array.isArray(oData)) {
				var err = "Disabling schema failed. Clear the following dependencies first:\n";
				for(var ind in oData) {
					err += "\t - " + oData[ind].name + " (v. " + oData[ind].version + ")";
				}
				sap.m.MessageBox.error(err)
			} else if(typeof(oData) === "object") {
				sap.m.MessageToast.show("Schema disabled.");
				if(window.location.hash != "#/schema") {
					window.location.hash = "#/schema"
				} else {
					SchemaService.getSchemaList(true, false)
				}
			}
		}, function() {
			sap.m.MessageBox.error("Failed to disable schema. Ensure no mapping tables or datasets use this schema(and/or version)")
		})	
	};
	
	this.createSchema = function(sName, sDescription) {
		Functions.ajax("api/schema/create", "POST", {
			name: sName,
			description: sDescription
		}, function(oData) {
			SchemaService.getSchemaList();
			model.setProperty("/currentSchema", oData)
			sap.m.MessageToast.show("Schema created.");
		}, function() {
			sap.m.MessageBox.error("Failed to create the schema, try reloading the application or try again later.")
		})	
	}
	
	this.isUniqueSchemaName = function(sName) {
		Functions.ajax("api/schema/isUniqueName/" + encodeURI(sName), "GET", {}, function(oData) {
			model.setProperty("/newSchema/nameUnique", oData)
		}, function() {
			sap.m.MessageBox.error("Failed to retreive isUniqueName. Please try again later.")
		})	
	}
}();