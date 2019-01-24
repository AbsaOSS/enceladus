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

var RuleService = new function () {

  var model = sap.ui.getCore().getModel();

  this.getRuleList = function (bLoadFirst) {
    Functions.ajax("api/rule/list", "GET", {}, function (oData) {
      model.setProperty("/rules", oData)
      if (oData.length > 0 && bLoadFirst)
        RuleService.getRuleVersion(oData[0]._id, oData[0].latestVersion)
    }, function () {
      sap.m.MessageBox
        .error("Failed to get the list of rules. Please wait a moment and try reloading the application")
    })
  };

  this.getLatestRuleVersion = function (sId) {
    Functions.ajax("api/rule/detail/" + encodeURI(sId) + "/latest", "GET", {}, function (oData) {
      model.setProperty("/currentRule", oData)
    }, function () {
      sap.m.MessageBox.error("Failed to get the detail of the rule. Please wait a moment and try reloading the application");
      window.location.hash = "#/rule"
    })
  };

  this.getRuleVersion = function (sId, iVersion, sModelPath) {
    var modelPath;
    if (sModelPath) modelPath = sModelPath;
    else modelPath = "/currentRule";
    Functions.ajax("api/rule/detail/" + encodeURI(sId) + "/" + encodeURI(iVersion), "GET", {}, function (oData) {
      model.setProperty(modelPath, oData)
    }, function () {
      sap.m.MessageBox.error("Failed to get the detail of the rule. Please wait a moment and try reloading the application");
      window.location.hash = "#/rule"
    })
  };

  this.disableRule = function (sId, iVersion) {
    var uri = "api/rule/disable/" + encodeURI(sId);
    if (typeof (iVersion) !== "undefined") {
      uri += "/" + encodeURI(iVersion)
    }

    Functions.ajax(uri, "GET", {}, function (oData) {
      if (Array.isArray(oData)) {
        var err = "Disabling rule failed. Clear the following dependencies first:\n";
        for (var ind in oData) {
          err += "\t - " + oData[ind].name + " (v. " + oData[ind].version + ")";
        }
        sap.m.MessageBox.error(err)
      } else if (typeof (oData) === "object") {
        sap.m.MessageToast.show("Rule disabled.");
        if (window.location.hash !== "#/rule") {
          window.location.hash = "#/rule"
        } else {
          RuleService.getRuleList(true, false)
        }
      }
    }, function () {
      sap.m.MessageBox.error("Failed to disable rule.")
    })
  };

  this.isUniqueRuleName = function (sName, model) {
    model.setProperty("/nameUsed", undefined);
    Functions.ajax("api/rule/isUniqueName/" + encodeURI(sName), "GET", {}, function (oData) {
      model.setProperty("/nameUnique", oData)
    }, function () {
      sap.m.MessageBox.error("Failed to retreive isUniqueName. Please try again later.")
    })
  };

  this.createRule = function (sDatasetName, oRule) {
    oRule.order = -1; // dummy for completeness

    if (oRule._t === "MappingConformanceRule") {
      oRule.attributeMappings = {};
      oRule.joinConditions.map(function(joinCondition) {
        oRule.attributeMappings[joinCondition.mappingTableField] = joinCondition.datasetField
      });
      delete oRule.joinConditions;
    }
    console.log(JSON.stringify(oRule)); // TODO: remove debug

    Functions.ajax("api/dataset/" + sDatasetName + "/rule/create", "POST", oRule,
      function (oData) {
      DatasetService.getDatasetList();
      SchemaService.getSchemaVersion(oData.schemaName, oData.schemaVersion, "/currentDataset/schema");
      model.setProperty("/currentDataset", oData);
      sap.m.MessageToast.show("Rule created.");
    }, function () {
      sap.m.MessageBox.error("Failed to create the rule, try reloading the application or try again later.")
    })
  };

  this.editRule = function (sName, iVersion, sDescription, sHdfsPath, sHdfsPublishPath, sSchemaName, iSchemaVersion) {
    Functions.ajax("api/dataset/edit", "POST", { // TODO: make this work
      name: sName,
      version: iVersion,
      description: sDescription,
      hdfsPath: sHdfsPath,
      hdfsPublishPath: sHdfsPublishPath,
      schemaName: sSchemaName,
      schemaVersion: iSchemaVersion
    }, function (oData) {
      RuleService.getRuleList();
      model.setProperty("/currentRule", oData);
      SchemaService.getSchemaVersion(oData.schemaName, oData.schemaVersion, "/currentDataset/schema");
      sap.m.MessageToast.show("Rule updated.");
    }, function () {
      sap.m.MessageBox.error("Failed to update the rule, try reloading the application or try again later.")
    })
  };
}();
