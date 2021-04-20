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

class JoinConditionDialog {

  constructor(oDialog, oController, model) {
    this._oDialog = oDialog;
    this._oController = oController;
    oController.byId("joinSubmitButton").attachPress(this.onJoinSubmit, this);
    oController.byId("joinCancelButton").attachPress(this.onJoinCancel, this);
    oController.byId("datasetSchemaFieldSelector").attachSelectionChange(this.onDatasetSchemaFieldSelect, this);
    oController.byId("mappingTableSchemaFieldSelector").attachSelectionChange(this.onMappingTableSchemaFieldSelect, this);
    this._datasetSchemaFieldSelector = new JoinConditionDatasetSchemaFieldSelector(oController, this.oDialog);
    this._mappingTableSchemaFieldSelector = new JoinConditionMappingTableSchemaFieldSelector(oController, this.oDialog);
    this._model = model;
  }

  get oDialog() {
    return this._oDialog;
  }

  get oController() {
    return this._oController;
  }

  get model() {
    return this._model;
  }

  get datasetSchemaFieldSelector() {
    return this._datasetSchemaFieldSelector;
  }

  get mappingTableSchemaFieldSelector() {
    return this._mappingTableSchemaFieldSelector;
  }

  get editIndex() {
    return this._editIndex;
  }

  set editIndex(index) {
    this._editIndex = index;
  }

  reset() {
    this.model.setProperty("/datasetField", "");
    this.model.setProperty("/mappingTableField", "");
    this.datasetSchemaFieldSelector.reset(this.oDialog);
    this.mappingTableSchemaFieldSelector.reset(this.oDialog);
  }

  onDatasetSchemaFieldSelect(oEv) {
    this.datasetSchemaFieldSelector.onSchemaFieldSelect(oEv, "/datasetField");
  }

  preselectDatasetField(datasetField) {
    this.model.setProperty("/datasetField", datasetField);
    this.datasetSchemaFieldSelector.preselectSchemaFieldSelector(datasetField);
  }

  onMappingTableSchemaFieldSelect(oEv) {
    this.mappingTableSchemaFieldSelector.onSchemaFieldSelect(oEv, "/mappingTableField");
  }

  preselectMappingTableField(mappingTableField) {
    this.model.setProperty("/mappingTableField", mappingTableField);
    this.mappingTableSchemaFieldSelector.preselectSchemaFieldSelector(mappingTableField);
  }

  setMappingTableSchema(schema) {
    const mappingTableSchemaModel = new sap.ui.model.json.JSONModel(schema);
    mappingTableSchemaModel.setSizeLimit(5000);
    this.oDialog.setModel(mappingTableSchemaModel, "mappingTableSchema");
  }

  setDatasetSchema(schema) {
    const datasetSchemaModel = new sap.ui.model.json.JSONModel(schema);
    datasetSchemaModel.setSizeLimit(5000);
    this.oDialog.setModel(datasetSchemaModel, "datasetSchema");
  }

  onJoinSubmit() {
    const join = {
      "datasetField" : this.model.getProperty("/datasetField"),
      "mappingTableField" : this.model.getProperty("/mappingTableField")
    };

    if (this.isValid(join)) {
      const joinConditionsPath = "/newRule/newJoinConditions";
      if (model.getProperty(joinConditionsPath) === undefined) {
        model.setProperty(joinConditionsPath, [])
      }
      const joins = model.getProperty(joinConditionsPath);
      if (this.editIndex === undefined) {
        joins.push(join);
      } else {
        joins[this.editIndex] = join;
        this.editIndex = undefined;
      }
      model.setProperty(joinConditionsPath, joins);
      this.onJoinCancel(); // close & clean up
    }
  }

  isValid({datasetField, mappingTableField}) {
    let isValid = true;

    const isDatasetFieldEmpty = GenericService.isEmpty(datasetField);
    const isMappingTableFieldEmpty = GenericService.isEmpty(mappingTableField);

    if (isDatasetFieldEmpty && isMappingTableFieldEmpty) {
      this.datasetSchemaFieldSelector.setErrorHighlight();
      this.mappingTableSchemaFieldSelector.setErrorHighlight();
      sap.m.MessageToast.show("No dataset or mapping table field selected.");
      isValid = false;
    } else if (isDatasetFieldEmpty) {
      this.datasetSchemaFieldSelector.setErrorHighlight();
      sap.m.MessageToast.show("No dataset field selected.");
      isValid = false;
    } else if (isMappingTableFieldEmpty) {
      this.mappingTableSchemaFieldSelector.setErrorHighlight();
      sap.m.MessageToast.show("No mapping table field selected.");
      isValid = false;
    }

    return isValid;
  }

  onJoinCancel() {
    this.oDialog.close();
    this.reset()
  }

  onAddPress() {
    this.oDialog.setBusy(true);
    this.oDialog.open();
    this.oDialog.setBusy(false);
  }

  onEditPress(index, datasetField, mappingTableField) {
    this.oDialog.setBusy(true);
    this.editIndex = index;
    this.oDialog.open();
    this.preselectDatasetField(datasetField);
    this.preselectMappingTableField(mappingTableField);
    this.oDialog.setBusy(false);
  }

}
