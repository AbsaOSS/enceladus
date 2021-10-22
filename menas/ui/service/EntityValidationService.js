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

var EntityValidationService = new function () {

  this.hasValidHDFSPath = function (sHDFSPath, sEntityType, oInput) {
    let rHDFSPathRegex = /^\/[\w\-\.\/=]+$/;
    let isOk = rHDFSPathRegex.test(sHDFSPath);

    if (!isOk) {
      let notOkReason = "can only contain alphanumeric characters, dot, underscore, dash or equals";

      if (sHDFSPath === "") { notOkReason = "cannot be empty" }
      if (sHDFSPath === "/") { notOkReason = "cannot be set to the root" }

      oInput.setValueState(sap.ui.core.ValueState.Error);
      oInput.setValueStateText(`${sEntityType} ${notOkReason}`);
    }

    return isOk;
  };

  // This is for cases when HDFS browser is disabled: entering an S3 path, etc.
  this.hasValidSimplePath = function (sHDFSPath, sEntityType, oInput) {
    let rHDFSPathRegex = /^[^\?\*]+$/;
    let isOk = rHDFSPathRegex.test(sHDFSPath);

    if (!isOk) {
      let notOkReason = "must not contain unwanted path chars like ? and *";
      if (sHDFSPath === "") { notOkReason = "cannot be empty" }

      oInput.setValueState(sap.ui.core.ValueState.Error);
      oInput.setValueStateText(`${sEntityType} ${notOkReason}`);
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
        sEntityType + " name can only contain alphanumeric characters and underscores");
      isOk = false;
    } else if (!oEntity.isEdit && !oEntity.nameUnique) {
      oInput.setValueState(sap.ui.core.ValueState.Error);
      oInput.setValueStateText(
        sEntityType + " name '" + oEntity.name + "' already exists. Choose a different name.");
      isOk = false;
    }

    return isOk;
  };

  this.hasValidSchema = function (oEntity, sEntityType, oVersionInput) {
    let isOk = true;

    if (!oEntity.schemaName || oEntity.schemaName === "") {
      isOk = false;
    }
    if (oEntity.schemaVersion === undefined || oEntity.schemaVersion === "") {
      oVersionInput.setValueState(sap.ui.core.ValueState.Error);
      oVersionInput.setValueStateText("Please choose the version of the Schema for the " + sEntityType);
      isOk = false;
    }

    return isOk;
  };

  this.hasValidProperties = function(aProperties, showDialogs) {
    let isOk = true;

    aProperties.map((oProp) => {
      const sDesc = oProp.description ? ` (${oProp.description})` : "";
      const isEnum = Functions.hasValidAllowedValues(oProp.propertyType);
      const msg = `${isEnum ? "Choose one of valid options for" : "Provide valid"} ${oProp.name}${sDesc}`;

      // check essentiality - if Mandatory property has no or empty value, we fail
      if (oProp.essentiality._t === "Mandatory" && !oProp.value) {
        isOk = false;
        oProp.validation = "Error";
        oProp.validationText = msg;
      } else if (oProp.essentiality._t === "Recommended" && !oProp.value) {
        oProp.validation = "Warning";
        oProp.validationText = msg;
      }

      // for enum type properties, check that the value is allowed or blank
      // (blank is not allowed for mandatory, but the above check takes care of that)
      if (isEnum && !oProp.propertyType.allowedValues.includes(oProp.value) && oProp.value !== "") {
        isOk = false;
        oProp.validation = "Error";
        oProp.validationText = msg;
      }
    });

    if (showDialogs && isOk) {
      // show warnings for unfilled Recommended properties but only if hasn't failed the validation already
      aProperties.map((oProp) => {
        if (oProp.essentiality._t === "Recommended" && !oProp.value) {
          if (isOk) {
            isOk = confirm(`Recommended property "${oProp.name}" does not have a defined value. Are you sure you want to continue?`);
          }
        }
      });
    }
    return isOk;
  };

}();
