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

var DatasetService = new function() {

	var model = sap.ui.getCore().getModel();

	this.getDatasetList = function(bLoadFirst) {
		Functions.ajax("api/dataset/list", "GET", {}, function(oData) {
			model.setProperty("/datasets", oData)
			if (oData.length > 0 && bLoadFirst)
				DatasetService.getDatasetVersion(oData[0]._id, oData[0].latestVersion)
		}, function() {
			sap.m.MessageBox
					.error("Failed to get the list of datasets. Please wait a moment and try reloading the application")
		})
	};

    this.getLatestDatasetVersion = function(sId) {
        Functions.ajax("api/dataset/detail/" + encodeURI(sId) + "/latest", "GET", {}, function(oData) {
            model.setProperty("/currentDataset", oData)
        }, function() {
            sap.m.MessageBox.error("Failed to get the detail of the dataset. Please wait a moment and try reloading the application");
            window.location.hash = "#/dataset"
        })
    };

    this.getDatasetVersion = function(sId, iVersion, sModelPath) {
        var modelPath;
        if(sModelPath) modelPath = sModelPath;
        else modelPath = "/currentDataset";
        Functions.ajax("api/dataset/detail/" + encodeURI(sId) + "/" + encodeURI(iVersion), "GET", {}, function(oData) {
            model.setProperty(modelPath, oData)
        }, function() {
            sap.m.MessageBox.error("Failed to get the detail of the dataset. Please wait a moment and try reloading the application");
            window.location.hash = "#/dataset"
        })
    };

    this.disableDataset = function(sId, iVersion) {
        var uri = "api/dataset/disable/" + encodeURI(sId);
        if(typeof(iVersion) !== "undefined") {
            uri += "/" + encodeURI(iVersion)
        }

        Functions.ajax(uri , "GET", {}, function(oData) {
            if(Array.isArray(oData)) {
                var err = "Disabling dataset failed. Clear the following dependencies first:\n";
                for(var ind in oData) {
                    err += "\t - " + oData[ind].name + " (v. " + oData[ind].version + ")";
                }
                sap.m.MessageBox.error(err)
            } else if(typeof(oData) === "object") {
                sap.m.MessageToast.show("Dataset disabled.");
                if(window.location.hash !== "#/dataset") {
                    window.location.hash = "#/dataset"
                } else {
                    DatasetService.getDatasetList(true, false)
                }
            }
        }, function() {
            sap.m.MessageBox.error("Failed to disable dataset.")
        })
    };

    this.isUniqueDatasetName = function(sName) {
        model.setProperty("/newDataset/nameUsed", undefined);
        Functions.ajax("api/dataset/isUniqueName/" + encodeURI(sName), "GET", {}, function(oData) {
            model.setProperty("/newDataset/nameUnique", oData)
        }, function() {
            sap.m.MessageBox.error("Failed to retreive isUniqueName. Please try again later.")
        })
    };

    this.createDataset = function(sName, sDescription, sHdfsPath, sHdfsPublishPath, sSchemaName, iSchemaVersion) {
        Functions.ajax("api/dataset/create", "POST", {
            name : sName,
            description : sDescription,
            hdfsPath : sHdfsPath,
            hdfsPublishPath : sHdfsPublishPath,
            schemaName : sSchemaName,
            schemaVersion : iSchemaVersion
        }, function(oData) {
            DatasetService.getDatasetList();
            SchemaService.getSchemaVersion(oData.schemaName, oData.schemaVersion, "/currentDataset/schema");
            model.setProperty("/currentDataset", oData);
            sap.m.MessageToast.show("Dataset created.");
        }, function() {
            sap.m.MessageBox.error("Failed to create the dataset, try reloading the application or try again later.")
        })
    };

    this.editDataset = function(sName, iVersion, sDescription, sHdfsPath, sHdfsPublishPath, sSchemaName, iSchemaVersion) {
        Functions.ajax("api/dataset/edit", "POST", {
            name : sName,
            version : iVersion,
            description : sDescription,
            hdfsPath : sHdfsPath,
            hdfsPublishPath : sHdfsPublishPath,
            schemaName : sSchemaName,
            schemaVersion : iSchemaVersion
        }, function(oData) {
            DatasetService.getDatasetList();
            model.setProperty("/currentDataset", oData);
            SchemaService.getSchemaVersion(oData.schemaName, oData.schemaVersion, "/currentDataset/schema");
            sap.m.MessageToast.show("Dataset updated.");
        }, function() {
            sap.m.MessageBox.error("Failed to update the dataset, try reloading the application or try again later.")
        })
    };
}();