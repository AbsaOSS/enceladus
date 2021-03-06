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


class EntityDialog {

  constructor(oDialog, entityService, oController) {
    this._oDialog = oDialog;
    this._entityService = entityService;
    this._oController = oController;
  }

  get oDialog() {
    return this._oDialog;
  }

  get entityService() {
    return this._entityService;
  }

  get oController() {
    return this._oController;
  }

  submit() {
    let newEntity = this.oDialog.getModel("entity").oData;
    if (!newEntity.isEdit && newEntity.name && typeof (newEntity.nameUnique) === "undefined") {
      // need to wait for the service call
      setTimeout(this.submit.bind(this), 500);
      return;
    }

    if (this.isValid(newEntity)) {
      // send and update UI
      if (newEntity.isEdit) {
        this.entityService.update(newEntity);
      } else {
        this.entityService.create(newEntity);
      }
      this.cancel(); // close & clean up
    }
  }

  cancel() {
    this.resetValueState();
    this.oDialog.close();
  }

  openSimpleOrHdfsBrowsingDialog(dialog, hdfsPropertyNames) {
    const hdfsPaths = hdfsPropertyNames.map(propertyName => dialog.getModel("entity").getProperty(propertyName));
    const hdfsCheckPromises = hdfsPaths.map(path => HdfsService.getHdfsListEs6Promise(path));

    // each propertyName is checked to be suitable for hdfsBrowser. Should any fail, hdfsBrowser will be disabled (hdfsBrowserEnabled=>false)
    Promise.all(hdfsCheckPromises) // all ok => ok, one fails => fail
      .then(() => {
        console.log(`Successful HDFS listing of '[${hdfsPaths}]' -> HDFS Browser is kept`);
      })
      .catch(() => {
        console.log(`Switching off HDFS Browser in the dialog due to an unsuccessful HDFS listing of '[${hdfsPaths}]'`); // 4xx or 5xx code
        dialog.getModel("entity").setProperty("/hdfsBrowserEnabled", false);
      })
      .finally(() => {
        dialog.open();
      })
  }

  onHdfsBrowserToggle() {
    let enabled = this.oDialog.getModel("entity").getProperty("/hdfsBrowserEnabled");
    this.oDialog.getModel("entity").setProperty("/hdfsBrowserEnabled", !enabled);
  }
}

class DatasetDialog extends EntityDialog {

  static hdfsPropertyNames = ["/hdfsPath", "/hdfsPublishPath"];

  constructor(oDialog, datasetService, schemaService, oController) {
    super(oDialog, datasetService, oController);
    this._schemaService = schemaService;
    oController.byId("newDatasetAddButton").attachPress(this.submit, this);
    oController.byId("newDatasetCancelButton").attachPress(this.cancel, this);
    oController.byId("newDatasetName").attachChange(this.onNameChange, this);

    oController.byId("toggleHdfsBrowser").attachPress(this.onHdfsBrowserToggle, this);

 }

  /**
   * Will create `oProps`'s allowedValues mapped into displayable sequence of objects, e.g.
   * ```["a","b"] -> [{"value":"a", "text": "a"},{"value":"b", "text":"b (suggested value)"}]```
   * @param oProp
   * @returns {undefined} or allowedValues sequence of Select-mappable object: (value, text)*
   */
  preprocessedAllowedValues(oProp) {
    if(Functions.hasValidAllowedValues(oProp.propertyType)) {
      let allowedMap = oProp.propertyType.allowedValues.map(val => {
        if (val == oProp.propertyType.suggestedValue) {
          return {value: val, text: `${val} (suggested value)`}
        } else {
          return {value: val, text: val}
        }
      });

      if (oProp.essentiality._t !== "Mandatory") {
        allowedMap = [{value: "", text: ""}, ...allowedMap] // (ES6 prepending) - ability to undefine the property
      }
        return allowedMap;

    } else {
      return undefined;
    }

  }

  get schemaService() {
    return this._schemaService;
  }

  resetValueState() {
    this.oController.byId("newDatasetName").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("newDatasetName").setValueStateText("");

    this.oController.byId("schemaVersionSelect").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("schemaVersionSelect").setValueStateText("");

    // hdfs browser-based
    this.oController.byId("selectedRawHDFSPathLabel").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("selectedRawHDFSPathLabel").setValueStateText("");

    this.oController.byId("selectedPublishHDFSPathLabel").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("selectedPublishHDFSPathLabel").setValueStateText("");

    // simple path-based
    this.oController.byId("newDatasetRawSimplePath").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("newDatasetRawSimplePath").setValueStateText("");

    this.oController.byId("newDatasetPublishSimplePath").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("newDatasetPublishSimplePath").setValueStateText("");

    //properties
    this.oDialog.getModel("entity").getProperty("/_properties").map(oProp => {
      oProp.validation = "None";
      oProp.validationText = "";
    });
    this.oDialog.getModel("entity").checkUpdate();
  }

  isValid(oDataset) {
    this.resetValueState();

    let hasValidName = EntityValidationService.hasValidName(oDataset, "Dataset",
      this.oController.byId("newDatasetName"));
    let hasValidSchema = EntityValidationService.hasValidSchema(oDataset, "Dataset",
        this.oController.byId("schemaVersionSelect"));

    //here the validation modifies the model's underlying data, trigger a check
    let hasValidProperties = EntityValidationService.hasValidProperties(oDataset._properties);
    this.oDialog.getModel("entity").checkUpdate();

    if (oDataset.hdfsBrowserEnabled) {
      let hasValidRawHDFSPath = EntityValidationService.hasValidHDFSPath(oDataset.hdfsPath,
        "Dataset Raw HDFS path",
        this.oController.byId("selectedRawHDFSPathLabel"));
      let hasValidPublishHDFSPath = EntityValidationService.hasValidHDFSPath(oDataset.hdfsPublishPath,
        "Dataset publish HDFS path",
        this.oController.byId("selectedPublishHDFSPathLabel"));
      let hasExistingRawHDFSPath = hasValidRawHDFSPath ? this.oController.byId("newDatasetRawHDFSBrowser").validate() : false;
      let hasExistingPublishHDFSPath = hasValidRawHDFSPath && hasValidPublishHDFSPath ?
        this.oController.byId("newDatasetPublishHDFSBrowser").validate() : false;

      return hasValidName && hasValidSchema && hasExistingRawHDFSPath && hasExistingPublishHDFSPath && hasValidProperties;
    } else {

      let hasValidRawSimplePath = EntityValidationService.hasValidSimplePath(oDataset.hdfsPath,
        "Dataset Raw path",
        this.oController.byId("newDatasetRawSimplePath"));
      let hasValidPublishSimplePath = EntityValidationService.hasValidSimplePath(oDataset.hdfsPublishPath,
        "Dataset publish path",
        this.oController.byId("newDatasetPublishSimplePath"));

      return hasValidName && hasValidSchema && hasValidRawSimplePath && hasValidPublishSimplePath && hasValidProperties;
    }
  }

  onNameChange() {
    let sName = this.oDialog.getModel("entity").getProperty("/name");
    if (GenericService.isValidEntityName(sName)) {
      DatasetService.hasUniqueName(sName, this.oDialog.getModel("entity"));
    } else {
      this.oDialog.getModel("entity").setProperty("/nameUnique", true);
    }
  }

  onSchemaSelect(oEv) {
    let sSchemaId = oEv.getParameter("selectedItem").getKey();
    this.schemaService.getAllVersions(sSchemaId, this.oController.byId("schemaVersionSelect"),
      this.oDialog.getModel("entity"), "/schemaVersion");
  }

  cancel() {
    sap.ui.getCore().getModel().setProperty("/currentSchemaVersions", undefined);
    super.cancel();
  }

  onPropertiesChange() {
    const inputFields = $(".propertyInput").control()
    const fnChangeHandler = function(oEv) {
      const oDataset = this.oDialog.getModel("entity").getProperty("/");
      this.resetValueState();
      this.isValid(oDataset)
    }.bind(this);
    inputFields.map((oInpField) => {
      //detach first in case these components are re-used
      oInpField.detachChange(fnChangeHandler);
      oInpField.attachChange(fnChangeHandler);
    });
  }

}

class AddDatasetDialog extends DatasetDialog {

  onPress() {
    const aPropsDef = sap.ui.getCore().getModel().getProperty("/properties");
    const aPropTemplate = aPropsDef.map ? aPropsDef : [];
    const aProps = aPropTemplate.map(oProp => {
      const oPreparedProp = jQuery.extend(true, {}, oProp);
      oPreparedProp.validation = "None";

      oPreparedProp.value = ""; // for Mandatory enums: this & forceSelection="false" on the <Select> results in no preselected value
      oPreparedProp.suggestedValue = oProp.propertyType.suggestedValue;

      // => e.g. [{"value":"a", "text": "a"},{"value":"b", "text":"b (suggested value)"}] or undefined for non-enums
      oPreparedProp.allowedValues = this.preprocessedAllowedValues(oProp);
      return oPreparedProp;
    });

    this.schemaService.getList(this.oDialog).then(() => {
      this.oDialog.setModel(new sap.ui.model.json.JSONModel({
        name: "",
        description: "",
        schemaName: "",
        schemaVersion: "",
        hdfsPath: "/",
        hdfsPublishPath: "/",
        isEdit: false,
        title: "Add",
        _properties: aProps,
        hdfsBrowserEnabled: true
      }), "entity");

      this.openSimpleOrHdfsBrowsingDialog(this.oDialog, DatasetDialog.hdfsPropertyNames)
    });

    //#1571 - This hack is to attach change handlers on inputs generated as a result of the above async data binding
    //Suggested approach described in #1668
    setTimeout(this.onPropertiesChange.bind(this), 1500);
  }

}

class EditDatasetDialog extends DatasetDialog {

  onPress() {
    const aPropsDef = sap.ui.getCore().getModel().getProperty("/properties");
    const aPropTemplate = aPropsDef.map ? aPropsDef : [];

    this.schemaService.getList(this.oDialog).then(() => {
      let current = this.oController._model.getProperty("/currentDataset");

      const aProps = aPropTemplate.map(oProp => {
        const oPreparedProp = jQuery.extend(true, {}, oProp);
        oPreparedProp.validation = "None";
        if (current.properties && current.properties[oPreparedProp.name]) {
          oPreparedProp.value = current.properties[oPreparedProp.name];
        } else {
          oPreparedProp.value = "";
          oPreparedProp.suggestedValue = oProp.propertyType.suggestedValue;
        }

        // => e.g. [{"value":"a", "text": "a"},{"value":"b", "text":"b (suggested value)"}] or undefined for non-enums
        oPreparedProp.allowedValues = this.preprocessedAllowedValues(oProp);

        return oPreparedProp;
      });

      current._properties = aProps;
      current.isEdit = true;
      current.title = "Edit";
      current.hdfsBrowserEnabled = true;

      this.schemaService.getAllVersions(current.schemaName, this.oController.byId("schemaVersionSelect"));
      this.oDialog.setModel(new sap.ui.model.json.JSONModel(jQuery.extend(true, {}, current)), "entity");

      this.openSimpleOrHdfsBrowsingDialog(this.oDialog, DatasetDialog.hdfsPropertyNames);

      //#1571 - This hack is to attach change handlers on inputs generated as a result of the above async data binding
      //Suggested approach described in #1668
      setTimeout(this.onPropertiesChange.bind(this), 1500);
    });
  }

}

class SchemaDialog extends EntityDialog {

  constructor(oDialog, schemaService, oController) {
    super(oDialog, schemaService, oController);
    oController.byId("newSchemaAddButton").attachPress(this.submit, this);
    oController.byId("newSchemaCancelButton").attachPress(this.cancel, this);
    oController.byId("newSchemaName").attachChange(this.onNameChange, this);
  }

  resetValueState() {
    this.oController.byId("newSchemaName").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("newSchemaName").setValueStateText("");
  }

  isValid(oSchema) {
    this.resetValueState();

    let hasValidName = EntityValidationService.hasValidName(oSchema, "Schema",
      this.oController.byId("newSchemaName"));

    return hasValidName;
  }

  onNameChange() {
    let sName = this.oDialog.getModel("entity").getProperty("/name");
    if (GenericService.isValidEntityName(sName)) {
      SchemaService.hasUniqueName(sName, this.oDialog.getModel("entity"))
    } else {
      this.oDialog.getModel("entity").setProperty("/nameUnique", true);
    }
  }

}

class AddSchemaDialog extends SchemaDialog {

  onPress() {
    this.oDialog.setModel(new sap.ui.model.json.JSONModel({
      name: "",
      description: "",
      isEdit: false,
      title: "Add"
    }), "entity");

    this.oDialog.open();
  }

}

class EditSchemaDialog extends SchemaDialog {

  onPress() {
    let current = this.oController._model.getProperty("/currentSchema");
    current.isEdit = true;
    current.title = "Edit";

    this.oDialog.setModel(new sap.ui.model.json.JSONModel(jQuery.extend(true, {}, current)), "entity");
    this.oDialog.open();
  }

}


class MappingTableDialog extends EntityDialog {
  static hdfsPropertyNames = ["/hdfsPath"];

  constructor(oDialog, mappingTableService, schemaService, oController) {
    super(oDialog, mappingTableService, oController);
    this._schemaService = schemaService;
    oController.byId("newMappingTableAddButton").attachPress(this.submit, this);
    oController.byId("newMappingTableCancelButton").attachPress(this.cancel, this);
    oController.byId("newMappingTableName").attachChange(this.onNameChange, this);

    oController.byId("toggleHdfsBrowser").attachPress(this.onHdfsBrowserToggle, this);
  }

  get schemaService() {
    return this._schemaService;
  }

  resetValueState() {
    this.oController.byId("newMappingTableName").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("newMappingTableName").setValueStateText("");

    this.oController.byId("schemaVersionSelect").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("schemaVersionSelect").setValueStateText("");

    // hdfs browser-based
    this.oController.byId("selectedHDFSPathLabel").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("selectedHDFSPathLabel").setValueStateText("");

    // simple path-based
    this.oController.byId("addMtSimplePath").setValueState(sap.ui.core.ValueState.None);
    this.oController.byId("addMtSimplePath").setValueStateText("");
  }

  isValid(oMT) {
    this.resetValueState();

    let hasValidName = EntityValidationService.hasValidName(oMT, "Mapping Table",
      this.oController.byId("newMappingTableName"));
    let hasValidSchema = EntityValidationService.hasValidSchema(oMT, "Mapping Table",
      this.oController.byId("schemaVersionSelect"));

    if (oMT.hdfsBrowserEnabled) {
      let hasValidHDFSPath = EntityValidationService.hasValidHDFSPath(oMT.hdfsPath,
        "Mapping Table HDFS path",
        this.oController.byId("selectedHDFSPathLabel"));
      let hasExistingHDFSPath = hasValidHDFSPath ? this.oController.byId("addMtHDFSBrowser").validate() : false;

      return hasValidName && hasValidSchema && hasExistingHDFSPath;
    } else {

      let hasValidSimplePath = EntityValidationService.hasValidSimplePath(oMT.hdfsPath,
        "Mapping Table path",
        this.oController.byId("addMtSimplePath"));

      return hasValidName && hasValidSchema && hasValidSimplePath;
    }
  }

  onNameChange() {
    let sName = this.oDialog.getModel("entity").getProperty("/name");
    if (GenericService.isValidEntityName(sName)) {
      MappingTableService.hasUniqueName(sName, this.oDialog.getModel("entity"));
    } else {
      this.oDialog.getModel("entity").setProperty("/nameUnique", true);
    }
  }
}

class AddMappingTableDialog extends MappingTableDialog {

  onPress() {
    this.schemaService.getList(this.oDialog).then(() => {
      this.oDialog.setModel(new sap.ui.model.json.JSONModel({
        name: "",
        description: "",
        schemaName: "",
        schemaVersion: "",
        hdfsPath: "/",
        isEdit: false,
        title: "Add",
        hdfsBrowserEnabled: true
      }), "entity");

      this.openSimpleOrHdfsBrowsingDialog(this.oDialog, MappingTableDialog.hdfsPropertyNames)
    });
  }

}

class EditMappingTableDialog extends MappingTableDialog {

  onPress() {
    this.schemaService.getList(this.oDialog).then(() => {
      const current = this.oController._model.getProperty("/currentMappingTable");

      current.isEdit = true;
      current.title = "Edit";
      current.hdfsBrowserEnabled = true;
      this.schemaService.getAllVersions(current.schemaName, this.oController.byId("schemaVersionSelect"));

      this.oDialog.setModel(new sap.ui.model.json.JSONModel(jQuery.extend(true, {}, current)), "entity");
      this.openSimpleOrHdfsBrowsingDialog(this.oDialog, MappingTableDialog.hdfsPropertyNames)
    });
  }

}
