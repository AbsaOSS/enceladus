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

var EntityValidationService = new function () {

  this.hasValidHDFSPaths = function (oEntity, sEntityType, oRawInput, oPublishInput) {
    let isRawPathOk = this._isValidHDFSPath(oEntity.hdfsPath, oRawInput);
    let isPublishPathOk = this._isValidHDFSPath(oEntity.hdfsPublishPath, oPublishInput);

    if (!isRawPathOk && !isPublishPathOk) {
      sap.m.MessageToast.show("Please choose the Raw and Publish HDFS paths of the " + sEntityType);
    } else if (!isRawPathOk) {
      sap.m.MessageToast.show("Please choose the Raw HDFS path of the " + sEntityType);
    } else if (!isPublishPathOk) {
      sap.m.MessageToast.show("Please choose the Publish HDFS path of the " + sEntityType);
    }

    return isRawPathOk && isPublishPathOk;
  };

  this.hasValidHDFSPath = function (oEntity, sEntityType, oInput) {
    let isOk = this._isValidHDFSPath(oEntity.hdfsPath, oInput);

    if (!isOk) {
      sap.m.MessageToast.show("Please choose the HDFS path of the " + sEntityType);
    }

    return isOk;
  };

  this.hasValidName = function (oEntity, sEntityType, oInput) {
    let isOk = true;

    if (!oEntity.name || oEntity.name === "") {
      oInput.setValueState(sap.ui.core.ValueState.Error);
      oInput.setValueStateText(sEntityType + " name cannot be empty");
      isOk = false;
    } else if (GenericService.hasWhitespace(oEntity.name)) {
      oInput.setValueState(sap.ui.core.ValueState.Error);
      oInput.setValueStateText(
        sEntityType + " name '" + oEntity.name + "' should not have whitespaces. Please remove spaces and retry");
      isOk = false;
    } else if (!oEntity.isEdit && !oEntity.nameUnique) {
      oInput.setValueState(sap.ui.core.ValueState.Error);
      oInput.setValueStateText(
        sEntityType + " name '" + oEntity.name + "' already exists. Choose a different name.");
      isOk = false;
    }

    return isOk;
  };

  this.hasValidSchema = function (oEntity, sEntityType, oNameInput, oVersionInput) {
    let isOk = true;

    if (!oEntity.schemaName || oEntity.schemaName === "") {
      oNameInput.setValueState(sap.ui.core.ValueState.Error);
      oNameInput.setValueStateText("Please choose the Schema of the " + sEntityType);
      isOk = false;
    }
    if (oEntity.schemaVersion === undefined || oEntity.schemaVersion === "") {
      oVersionInput.setValueState(sap.ui.core.ValueState.Error);
      oVersionInput.setValueStateText("Please choose the version of the Schema for the " + sEntityType);
      isOk = false;
    }

    return isOk;
  };

  this._isValidHDFSPath = function(sPath, oInput) {
    let isOk = true;
    if (!sPath || sPath === "/") {
      oInput.setValueState(sap.ui.core.ValueState.Error);
      isOk = false;
    }

    return isOk;
  };

}();
