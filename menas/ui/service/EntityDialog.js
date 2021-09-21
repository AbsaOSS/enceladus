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
    console.log(`submitted entity: ${JSON.stringify(newEntity)}`);

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
    if (Functions.hasValidAllowedValues(oProp.propertyType)) {
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

  isValidWithDialogSwitch(oDataset, showDialogs) {
    this.resetValueState();

    let hasValidName = EntityValidationService.hasValidName(oDataset, "Dataset",
      this.oController.byId("newDatasetName"));
    let hasValidSchema = EntityValidationService.hasValidSchema(oDataset, "Dataset",
      this.oController.byId("schemaVersionSelect"));

    //here the validation modifies the model's underlying data, trigger a check
    let hasValidProperties = EntityValidationService.hasValidProperties(oDataset._properties, showDialogs);
    this.oDialog.getModel("entity").checkUpdate();

    if (oDataset.hdfsBrowserEnabled) {
      let hasValidRawHDFSPath = EntityValidationService.hasValidHDFSPath(oDataset.hdfsPath,
        "Dataset Raw HDFS path",
        this.oController.byId("selectedRawHDFSPathLabel"));
      let hasValidPublishHDFSPath = EntityValidationService.hasValidHDFSPath(oDataset.hdfsPublishPath,
        "Dataset publish HDFS path",
        this.oController.byId("selectedPublishHDFSPathLabel"));
      let hasExistingRawHDFSPath = showDialogs && hasValidProperties && hasValidRawHDFSPath ?
        this.oController.byId("newDatasetRawHDFSBrowser").validate() : false;
      let hasExistingPublishHDFSPath = showDialogs && hasValidProperties && hasExistingRawHDFSPath && hasValidPublishHDFSPath ?
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

  isValid(oDataset) {
    return this.isValidWithDialogSwitch(oDataset, true);
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
      this.isValidWithDialogSwitch(oDataset, false);
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

  submit() {
    let newEntity = this.oDialog.getModel("entity").oData;
    const updatedFilters = newEntity.updatedFilters;

    if (updatedFilters) {
      if (updatedFilters.length > 1) {
        console.error(`Multiple root filters found, aborting: ${JSON.stringify(updatedFilters)}`);
        sap.m.MessageToast.show("Invalid filter update found (multiple roots), no filter update done");
      } else {
        let updatedFilter = this.removeNiceNamesFromFilterData(updatedFilters[0]);

        this.oDialog.getModel("entity").setProperty("/filter", updatedFilter);
        console.debug(`Submitted MT entity after filters replace: ${JSON.stringify(this.oDialog.getModel("entity").oData)}`);
      }
    } // do nothing on empty filter

    super.submit()
  }

  constructor(oDialog, mappingTableService, schemaService, oController) {
    super(oDialog, mappingTableService, oController);
    this._schemaService = schemaService;
    oController.byId("newMappingTableAddButton").attachPress(this.submit, this);
    oController.byId("newMappingTableCancelButton").attachPress(this.cancel, this);
    oController.byId("newMappingTableName").attachChange(this.onNameChange, this);

    // filter toolbar:
    oController.byId("addAndBtn").attachPress(this.onFilterAddAnd, this);
    oController.byId("addOrBtn").attachPress(this.onFilterAddOr, this);
    oController.byId("addNotBtn").attachPress(this.onFilterAddNot, this);
    oController.byId("addEqualsBtn").attachPress(this.onFilterAddEquals, this);
    oController.byId("addDiffersBtn").attachPress(this.onFilterAddDiffers, this);
    oController.byId("addIsNullBtn").attachPress(this.onFilterAddIsNull, this);
    oController.byId("removeSelectedBtn").attachPress(this.onRemoveSelected, this);

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

    this.resetFilterValidation()
  }

  /**
   * This method resets validations on the UI
   */
  resetFilterValidation() {
    const treeTable = this.oController.byId("filterTreeEdit");
    const treeTableModel = treeTable.getBinding().getModel();

    const filterData = treeTableModel.getProperty("/updatedFilters");
    console.log(`filterTreeOData = ${JSON.stringify(filterData)}`);

    // filter data can be [filter], [null] or null
    if (filterData && filterData.map(x => x).length != 0) {
      // resetting non-empty filter validations

      const resetValidatedFilter = this.resetFilterDataValidation(filterData[0]);
      treeTableModel.setProperty("/updatedFilters", [resetValidatedFilter]);
    }
  }

  /**
   * This method operates on the data-object to immutably reset it (creates a copy with the reset validation fields)
   * @param filterData
   * @returns {copy} with reset validations
   */
  resetFilterDataValidation(filterData) {

    // fn to add human readable text
    const applyFn = function (filterNode) {
      switch (filterNode._t) {
        case "AndJoinedFilters":
        case "OrJoinedFilters":
        case "NotFilter":
          filterNode.filter_valueState = "None";
          filterNode.filter_valueStateText = "";

          break;
        case "IsNullFilter":
          filterNode.filter_valueState = "None";
          filterNode.filter_valueStateText = "";

          filterNode.columnName_valueState = "None";
          filterNode.columnName_valueStateText = "";
          break;

        case "EqualsFilter":
        case "DiffersFilter":
          filterNode.filter_valueState = "None";
          filterNode.filter_valueStateText = "";

          filterNode.columnName_valueState = "None";
          filterNode.columnName_valueStateText = "";

          filterNode.value_valueState = "None";
          filterNode.value_valueStateText = "";

          filterNode.valueType_valueState = "None";
          filterNode.valueType_valueStateText = "";
          break;
        default:
      }
    };

    return FilterTreeUtils.applyToFilterDataImmutably(filterData, applyFn);
  }

  isValid(oMT) {
    this.resetValueState(); // includes reset of filter validation

    let hasValidName = EntityValidationService.hasValidName(oMT, "Mapping Table",
      this.oController.byId("newMappingTableName"));
    let hasValidSchema = EntityValidationService.hasValidSchema(oMT, "Mapping Table",
      this.oController.byId("schemaVersionSelect"));

    const treeTable = this.oController.byId("filterTreeEdit");
    const treeTableModel = treeTable.getBinding().getModel();

    // todo move to "filterData validation fn?"
    const filterData = treeTableModel.getProperty("/updatedFilters");
    console.log(`filterTreeOData = ${JSON.stringify(filterData)}`);

    let hasValidFilter = true;
    // filter data can be [filter], [null] or null
    if (!filterData || filterData.map(x => x).length == 0) {
       hasValidFilter = true;
    } else {
      // validate filter tree
      const validateInUiFn = function (filterNode) {
        switch (filterNode._t) {
          case "AndJoinedFilters":
          case "OrJoinedFilters":
            if (filterNode.filterItems.map(x => x).length == 0) { // empty deleted ([null]) is not valid
              filterNode.filter_valueState = "Error";
              filterNode.filter_valueStateText = "Container filter must contain child filters!";
              hasValidFilter = false;
            }
            break;

          case "NotFilter":
            if (!filterNode.inputFilter) {
              filterNode.filter_valueState = "Error";
              filterNode.filter_valueStateText = "Container filter must contain a child filter!";
              hasValidFilter = false;
            }
            break;

           case "EqualsFilter":
           case "DiffersFilter":
             if (filterNode.columnName.length == 0) {
               filterNode.columnName_valueState = "Error";
               filterNode.columnName_valueStateText = "Fill in the column name.";
               hasValidFilter = false;
             }

             if (filterNode.value.length == 0) {
               filterNode.value_valueState = "Error";
               filterNode.value_valueStateText = "Fill in the value.";
               hasValidFilter = false;
             }

             if (filterNode.valueType.length == 0) {
               filterNode.valueType_valueState = "Error";
               filterNode.valueType_valueStateText = "Fill in value type.";
               hasValidFilter = false;
             }
             break;

          case "IsNullFilter":
            if (filterNode.columnName.length == 0) {
              filterNode.columnName_valueState = "Error";
              filterNode.columnName_valueStateText = "Fill in column name.";
              hasValidFilter = false;
            }
            break;

          default:
        }
      };

      const validatedFilter = FilterTreeUtils.applyToFilterDataImmutably(filterData[0], validateInUiFn);
      treeTableModel.setProperty("/updatedFilters", [validatedFilter]);
      treeTableModel.refresh();
    }

    if (oMT.hdfsBrowserEnabled) {
      let hasValidHDFSPath = EntityValidationService.hasValidHDFSPath(oMT.hdfsPath,
        "Mapping Table HDFS path",
        this.oController.byId("selectedHDFSPathLabel"));
      let hasExistingHDFSPath = hasValidHDFSPath ? this.oController.byId("addMtHDFSBrowser").validate() : false;

      return hasValidName && hasValidSchema && hasExistingHDFSPath && hasValidFilter;
    } else {

      let hasValidSimplePath = EntityValidationService.hasValidSimplePath(oMT.hdfsPath,
        "Mapping Table path",
        this.oController.byId("addMtSimplePath"));

      return hasValidName && hasValidSchema && hasValidSimplePath && hasValidFilter;
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

  onFilterAddAnd() {
    this.onFilterAdd({_t: "AndJoinedFilters", filterItems: []})
  }

  onFilterAddOr() {
    this.onFilterAdd({_t: "OrJoinedFilters", filterItems: []})
  }

  onFilterAddNot() {
    this.onFilterAdd({_t: "NotFilter", inputFilter: null})
  }

  onFilterAddEquals() {
    this.onFilterAdd({_t: "EqualsFilter", columnName: "", value: "", valueType: ""})
  }

  onFilterAddDiffers() {
    this.onFilterAdd({_t: "DiffersFilter", columnName: "", value: "", valueType: ""})
  }

  onFilterAddIsNull() {
    this.onFilterAdd({_t: "IsNullFilter", columnName: ""})
  }

  addNiceNamesToFilterData(filterData) {

    // fn to add human readable text
    const applyFn = function (filterNode) {
      switch (filterNode._t) {
        case "AndJoinedFilters":
          filterNode.text = "AND";
          break;
        case "OrJoinedFilters":
          filterNode.text = "OR";
          break;
        case "EqualsFilter":
          filterNode.text = "Equals";
          break;
        case "DiffersFilter":
          filterNode.text = `Differs`;
          break;
        case "NotFilter":
          filterNode.text = "NOT";
          break;
        case "IsNullFilter":
          filterNode.text = `is NULL`;
          break;
        default:
      }
    };

    return FilterTreeUtils.applyToFilterDataImmutably(filterData, applyFn);
  }

  removeNiceNamesFromFilterData(filterData) {

    // fn to add human readable text
    const applyFn = function (filterNode) {
      filterNode.text = undefined;
    };

    return FilterTreeUtils.applyToFilterDataImmutably(filterData, applyFn);
  }

  onFilterAdd(blankFilter) {
    // blank filter contains validation fields:
    const namedBlankFilter = this.resetFilterDataValidation(this.addNiceNamesToFilterData(blankFilter));

    const treeTable = this.oController.byId("filterTreeEdit");
    const selectedIndices = treeTable.getSelectedIndices();
    const treeTableModel = treeTable.getBinding().getModel();

    const currentFilters = this.oDialog.getModel("entity").getProperty("/updatedFilters");
    const filtersEmpty = !currentFilters || currentFilters.filter(x => x).length == 0; // after removal of previous, there can be [null]

    if (filtersEmpty) {
      treeTableModel.setProperty("/updatedFilters", [namedBlankFilter]); // add first filter by replacing the empty model

    } else if (selectedIndices.length == 1) {
      const newParentContext = treeTable.getContextByIndex(selectedIndices[0]);
      const newParent = newParentContext.getProperty();

      // based on what type of filter is selected, attach the new filter to it
      if (newParent._t == 'AndJoinedFilters' || newParent._t == 'OrJoinedFilters' ) { //and / or -> add
        newParent.filterItems = newParent.filterItems.concat(namedBlankFilter)
      } else if (newParent._t == 'NotFilter') {
        newParent.inputFilter = namedBlankFilter // not -> replace
      } else {
        sap.m.MessageToast.show("Could not add filter. Select AND, OR or NOT can have child filter added to. ");
        return;
      }
    } else {

      sap.m.MessageToast.show("Select exactly one item to add a child to!");
      return;
    }

    treeTableModel.refresh();
    if (selectedIndices) {
      treeTable.expand(selectedIndices[0]); // nice of the user to directly see the child among the expanded parent
    }
  }

  onRemoveSelected() {
    const treeTable = this.oController.byId("filterTreeEdit");
    const selectedIndices = treeTable.getSelectedIndices();
    const treeTableModel = treeTable.getBinding().getModel();

    if (selectedIndices.length === 0) {
      sap.m.MessageToast.show("Select one or more items to remove.");
      return;
    }

    // delete the data.
    selectedIndices.forEach(idx => {
        const context = treeTable.getContextByIndex(idx);
        const data = context.getProperty();

        if (data) {
          // The property is simply set to undefined to preserve the tree state (expand/collapse states of nodes).
          treeTableModel.setProperty(context.getPath(), undefined, context, true);
        }
      }
    );
  }

  // on MTDialog open - base
  onPress() {
    const suggestedColumnTypes = FilterTreeUtils.columnTypeNames.map(function (val) {
      return {name: val}
    }); // [ {name: string}, ...]

    const typeModel = new sap.ui.model.json.JSONModel(suggestedColumnTypes);
    this.oDialog.setModel(typeModel, "suggestedColumnTypes");
  }
}

class AddMappingTableDialog extends MappingTableDialog {

  onPress() {
    super.onPress();

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
    super.onPress();

    this.schemaService.getList(this.oDialog).then(() => {
      const current = this.oController._model.getProperty("/currentMappingTable");

      current.updatedFilters = [this.addNiceNamesToFilterData(this.resetFilterDataValidation(current.filter))];
      console.log(`current filters: ${JSON.stringify(current.updatedFilters)}`);

      current.isEdit = true;
      current.title = "Edit";
      current.hdfsBrowserEnabled = true;
      this.schemaService.getAllVersions(current.schemaName, this.oController.byId("schemaVersionSelect"));

      const model = new sap.ui.model.json.JSONModel(jQuery.extend(true, {}, current));
      this.oDialog.setModel(model, "entity");

      this.openSimpleOrHdfsBrowsingDialog(this.oDialog, MappingTableDialog.hdfsPropertyNames)
    });
  }

}
