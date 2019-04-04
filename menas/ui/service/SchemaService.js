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
var SchemaService = new function () {

  let model = sap.ui.getCore().getModel();
  let eventBus = sap.ui.getCore().getEventBus();

  this.updateMasterPage = function () {
    eventBus.publish("schemas", "list");
  };

  this.getSchemaList = function (oControl, fnOnComplete) {
    Functions.ajax("api/schema/list", "GET", {}, (oData) => {
      oControl.setModel(new sap.ui.model.json.JSONModel(oData), "schemas");
      console.log("done");
      if (fnOnComplete) {
        fnOnComplete(oData);
      }
    }, () => {
      sap.m.MessageBox.error("Failed to get the list of schemas. Please wait a moment and try reloading the application")
    })
  };

  this.getFirstSchema = function () {
    Functions.ajax("api/schema/list", "GET", {}, (oData) => {
      SchemaService.getSchemaVersion(oData[0]._id, oData[0].latestVersion)
    }, () => {
      sap.m.MessageBox.error("Failed to get any schemas. Please wait a moment and try reloading the application")
    })
  };

  this.getLatestSchemaVersion = function (sId) {
    Functions.ajax("api/schema/detail/" + encodeURI(sId) + "/latest", "GET", {}, (oData) => {
      model.setProperty("/currentSchema", oData);
      SchemaService.getSchemaUsedIn(oData.name, oData.version);
      SchemaService.getAuditTrail(oData.name);
    }, () => {
      sap.m.MessageBox.error("Failed to get the detail of the schema. Please wait a moment and try reloading the application")
      window.location.hash = "#/schema"
    })
  };

  this.getAuditTrail = function (sId) {
    Functions.ajax("api/schema/detail/" + encodeURI(sId) + "/audit", "GET", {}, (oData) => {
      model.setProperty("/currentSchema/auditTrail", oData)
    }, () => {
      sap.m.MessageBox.error("Failed to get the audit trail of the schema. Please wait a moment and/or try reloading the application")
    })
  };

  this.getSchemaVersion = function (sId, iVersion, sModelPath) {
    let modelPath = (sModelPath) ? sModelPath : "/currentSchema";

    Functions.ajax("api/schema/detail/" + encodeURI(sId) + "/" + encodeURI(iVersion), "GET", {}, (oData) => {
      model.setProperty(modelPath, oData);
      SchemaService.getSchemaUsedIn(oData.name, oData.version);
      SchemaService.getAuditTrail(oData.name)
    }, () => {
      sap.m.MessageBox.error("Failed to get the detail of the schema. Please wait a moment and try reloading the application")
      window.location.hash = "#/schema"
    })
  };

  this.updateSchema = function (sId, iVersion, sDesc) {
    Functions.ajax("api/schema/edit", "POST", {
      name: sId,
      version: iVersion,
      description: sDesc
    }, (oData) => {
      model.setProperty("/currentSchema", oData);
      SchemaService.getAuditTrail(oData.name);
      this.updateMasterPage();
    }, () => {
      sap.m.MessageBox.error("Failed to update the schema. Please wait a moment and try reloading the application")
    })
  };

  this.getSchemaUsedIn = function (sId, iVersion) {
    Functions.ajax("api/schema/usedIn/" + encodeURI(sId) + "/" + encodeURI(iVersion), "GET", {}, (oData) => {
      model.setProperty("/currentSchema/usedIn", oData)
    }, () => {
      sap.m.MessageBox.error("Failed to retreive the 'Used In' section, please try again later.")
    })
  };

  this.getAllSchemaVersions = function (sName, oControl, oModel, sProperty) {
    if (oControl)
      oControl.setBusy(true);
    Functions.ajax("api/schema/allVersions/" + encodeURI(sName), "GET", {}, (oData) => {
      model.setProperty("/currentSchemaVersions", oData);
      if (oControl) {
        oControl.setBusy(false);
      }
      if (oModel && sProperty) {
        oModel.setProperty(sProperty, oData[oData.length - 1].version)
      }
    }, () => {
      sap.m.MessageBox.error("Failed to retreive all versions of the schema, please try again later.")
      oControl.setBusy(false);
    }, oControl)
  };

  this.disableSchema = function (sId, iVersion) {
    let uri = "api/schema/disable/" + encodeURI(sId);
    if (typeof (iVersion) !== "undefined") {
      uri += "/" + encodeURI(iVersion)
    }

    Functions.ajax(uri, "GET", {}, (oData) => {
      sap.m.MessageToast.show("Schema disabled.");
      this.updateMasterPage();

      if (window.location.hash !== "#/schema") {
        window.location.hash = "#/schema"
      } else {
        SchemaService.getFirstSchema();
      }
    }, function (xhr) {
      if (xhr.status === 400) {
        let oData = JSON.parse(xhr.responseText);

        let err = EntityService.buildDisableFailureMsg(oData, "Dataset");

        sap.m.MessageBox.error(err)
      } else {
        sap.m.MessageBox.error("Failed to disable schema. Ensure no mapping tables or datasets use this schema(and/or version)")
      }
    })
  };

  this.createSchema = function (sName, sDescription) {
    Functions.ajax("api/schema/create", "POST", {
      name: sName,
      description: sDescription
    }, (oData) => {
      eventBus.publish("schemas", "created", oData);
      sap.m.MessageToast.show("Schema created.");
    }, () => {
      sap.m.MessageBox.error("Failed to create the schema, try reloading the application or try again later.")
    })
  };

  this.hasUniqueName = function (sName) {
    Functions.ajax("api/schema/isUniqueName/" + encodeURI(sName), "GET", {}, (oData) => {
      model.setProperty("/newSchema/nameUnique", oData)
    }, () => {
      sap.m.MessageBox.error("Failed to retreive isUniqueName. Please try again later.")
    })
  };

  this.fieldSelect = function (sBindingPath, sModelPathBase, oModel, sOutputProperty) {
    model.setProperty(sOutputProperty, this._buildSchemaPath(sBindingPath, sModelPathBase, oModel));
  };

  this._buildSchemaPath = function (sBindingPath, sModelPathBase, oModel) {
    let pathToks = sBindingPath.replace(sModelPathBase, "").split("/");

    let helper = function (aToks, sModelPathAcc, aAcc) {
      if (aToks.length === 0) {
        return aAcc.join(".");
      }

      let rev = aToks.reverse();
      let sCurrPath = sModelPathAcc + rev.pop() + "/";
      let curr = oModel.getProperty(sCurrPath);
      aAcc.push(curr.name);

      let newPath = sCurrPath + rev.pop() + "/";

      return helper(rev.reverse(), newPath, aAcc)
    };

    return helper(pathToks, sModelPathBase, [])
  }
}();
