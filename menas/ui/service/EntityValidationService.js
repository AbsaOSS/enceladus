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

  this.hasValidProperties = function(aProperties) {
    console.log(aProperties);

    let isOk = true;

    aProperties.map((oProp) => {
      //check essentiality - if Mandatory property has no or empty value, we fail
      if(oProp.essentiality._t === "Mandatory" && !oProp.value) {
        isOk = false;
        oProp.validation = "Error";
        oProp.validationText = `Provide valid ${oProp.name} - ${oProp.description}`;
      }
      //for enum type properties, check that the value is allowed
      if(oProp.propertyType.allowedValues && oProp.propertyType.allowedValues.length > 0) {
        if(oProp.propertyType.allowedValues.filter((oVal) => { return oVal.value === oProp.value }).length === 0) {
          isOk = false;
          oProp.validation = "Error";
          oProp.validationText = `Choose one of valid options for ${oProp.name} - ${oProp.description}`;
        }
      }
    })

    return isOk;
  };

}();
