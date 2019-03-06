/*
 * Copyright 2018-2019 ABSA Group Limited
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
          DatasetService.setCurrentDataset(oData);
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
      let uri = "api/dataset/disable/" + encodeURI(sId);
      if(typeof(iVersion) !== "undefined") {
          uri += "/" + encodeURI(iVersion)
      }

      Functions.ajax(uri , "GET", {}, function(oData) {
        sap.m.MessageToast.show("Dataset disabled.");
        if(window.location.hash !== "#/dataset") {
            window.location.hash = "#/dataset"
        } else {
            DatasetService.getDatasetList(true, false)
        }
      }, function(xhr) {
        if (xhr.status === 400) {
          let err = "Disabling dataset failed. Clear the following dependencies first:\n";
          let oData = JSON.parse(xhr.responseText);
          for(let ind in oData) {
            err += "\t - " + oData[ind].name + " (v. " + oData[ind].version + ")";
          }
          sap.m.MessageBox.error(err)
        } else {
          sap.m.MessageBox.error("Failed to disable dataset.")
        }
      })
    };

    this.isNameUnique = function(sName, oModel) {
        GenericService.isNameUnique(sName, oModel, "dataset")
    };

    this.createDataset = function(oDataset) {
        Functions.ajax("api/dataset/create", "POST", oDataset, function(oData) {
            DatasetService.getDatasetList();
            SchemaService.getSchemaVersion(oData.schemaName, oData.schemaVersion, "/currentDataset/schema");
            DatasetService.setCurrentDataset(oData);
            sap.m.MessageToast.show("Dataset created.");
        }, function() {
            sap.m.MessageBox.error("Failed to create the dataset, try reloading the application or try again later.")
        })
    };

    this.editDataset = function(oDataset) {
      Functions.ajax("api/dataset/edit", "POST", oDataset, function(oData) {
            DatasetService.getDatasetList();
            DatasetService.setCurrentDataset(oData);
            SchemaService.getSchemaVersion(oData.schemaName, oData.schemaVersion, "/currentDataset/schema");
            sap.m.MessageToast.show("Dataset updated.");
        }, function() {
            sap.m.MessageBox.error("Failed to update the dataset, try reloading the application or try again later.")
        })
    };

  this.setCurrentDataset = function(oDataset) {
    oDataset.conformance = oDataset.conformance.sort((first, second) => first.order > second.order);
    model.setProperty("/currentDataset", oDataset);
  };

}();
