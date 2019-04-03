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

var DatasetService = new function () {

  let model = sap.ui.getCore().getModel();
  let eventBus = sap.ui.getCore().getEventBus();

  this.updateMasterPage = function () {
    eventBus.publish("dataset", "list");
  };

  this.getDatasetList = function (oControl) {
    Functions.ajax("api/dataset/list", "GET", {}, (oData) => {
      oControl.setModel(new sap.ui.model.json.JSONModel(oData), "datasets");
    }, () => {
      sap.m.MessageBox.error("Failed to get the list of datasets. Please wait a moment and try reloading the application")
    })
  };

  this.getFirstDataset = function () {
    Functions.ajax("api/dataset/list", "GET", {}, (oData) => {
      DatasetService.getDatasetVersion(oData[0]._id, oData[0].latestVersion);
    }, () => {
      sap.m.MessageBox.error("Failed to get any dataset. Please wait a moment and try reloading the application")
    })
  };

  this.getLatestDatasetVersion = function (sId) {
    Functions.ajax("api/dataset/detail/" + encodeURI(sId) + "/latest", "GET", {}, (oData) => {
      DatasetService.setCurrentDataset(oData);
      DatasetService.getAuditTrail(oData["name"]);
    }, () => {
      sap.m.MessageBox.error("Failed to get the detail of the dataset. Please wait a moment and try reloading the application");
      window.location.hash = "#/dataset"
    })
  };

  this.getDatasetVersion = function (sId, iVersion, sModelPath) {
    var modelPath;
    if (sModelPath) modelPath = sModelPath;
    else modelPath = "/currentDataset";
    Functions.ajax("api/dataset/detail/" + encodeURI(sId) + "/" + encodeURI(iVersion), "GET", {}, (oData) => {
      model.setProperty(modelPath, oData);
      DatasetService.getAuditTrail(oData["name"]);
    }, () => {
      sap.m.MessageBox.error("Failed to get the detail of the dataset. Please wait a moment and try reloading the application");
      window.location.hash = "#/dataset"
    })
  };

  this.getAuditTrail = function (sId) {
    Functions.ajax("api/dataset/detail/" + encodeURI(sId) + "/audit", "GET", {}, (oData) => {
      model.setProperty("/currentDataset/auditTrail", oData)
    }, () => {
      sap.m.MessageBox.error("Failed to get the audit trail of the dataset. Please wait a moment and/or try reloading the application")
    })
  };

  this.disableDataset = function (sId, iVersion) {
    let uri = "api/dataset/disable/" + encodeURI(sId);
    if (typeof (iVersion) !== "undefined") {
      uri += "/" + encodeURI(iVersion)
    }

    Functions.ajax(uri, "GET", {}, (oData) => {
      sap.m.MessageToast.show("Dataset disabled.");
      this.updateMasterPage();

      if (window.location.hash !== "#/dataset") {
        window.location.hash = "#/dataset";
      } else {
        DatasetService.getFirstDataset();
      }
    }, (xhr) => {
      sap.m.MessageBox.error("Failed to disable dataset.")
    })
  };

  this.hasUniqueName = function (sName, oModel) {
    GenericService.isNameUnique(sName, oModel, "dataset")
  };

  this.createDataset = function (oDataset) {
    Functions.ajax("api/dataset/create", "POST", oDataset, (oData) => {
      this.updateMasterPage();
      SchemaService.getSchemaVersion(oData.schemaName, oData.schemaVersion, "/currentDataset/schema");
      DatasetService.setCurrentDataset(oData);
      DatasetService.getAuditTrail(oData["name"]);
      sap.m.MessageToast.show("Dataset created.");
    }, () => {
      sap.m.MessageBox.error("Failed to create the dataset, try reloading the application or try again later.")
    })
  };

  this.editDataset = function (oDataset) {
    Functions.ajax("api/dataset/edit", "POST", oDataset, (oData) => {
      this.updateMasterPage();
      DatasetService.setCurrentDataset(oData);
      DatasetService.getAuditTrail(oData["name"]);
      SchemaService.getSchemaVersion(oData.schemaName, oData.schemaVersion, "/currentDataset/schema");
      sap.m.MessageToast.show("Dataset updated.");
    }, () => {
      sap.m.MessageBox.error("Failed to update the dataset, try reloading the application or try again later.")
    })
  };

  this.setCurrentDataset = function (oDataset) {
    oDataset.conformance = oDataset.conformance.sort((first, second) => first.order > second.order);
    model.setProperty("/currentDataset", oDataset);
  };

}();
