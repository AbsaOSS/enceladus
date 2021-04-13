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

class OutputColumnDialog {

  constructor(oDialog, oController, model) {
    this._oDialog = oDialog;
    this._oController = oController;
    oController.byId("outputColumnSubmitButton").attachPress(this.onNewColumnSubmit, this);
    oController.byId("outputColumnCancelButton").attachPress(this.onNewColumnCancel, this);
    oController.byId("targetAttributeSelector").attachSelectionChange(this.onMappingTableSchemaFieldSelect, this);
    this._mappingTableSchemaFieldSelector = new OutputColumnMappingTableSchemaFieldSelector(oController, this.oDialog);
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
    this.model.setProperty("/targetAttribute", "");
    this.model.setProperty("/outputColumn", "");
    this.mappingTableSchemaFieldSelector.reset(this.oDialog);
  }

  setMappingTableSchema(schema) {
    const mappingTableSchemaModel = new sap.ui.model.json.JSONModel(schema);
    mappingTableSchemaModel.setSizeLimit(5000);
    this.oDialog.setModel(mappingTableSchemaModel, "mappingTableSchema");
  }

  onMappingTableSchemaFieldSelect(oEv) {
    this.mappingTableSchemaFieldSelector.onSchemaFieldSelect(oEv, "/targetAttribute");
  }

  preselectMappingTableField(targetAttribute) {
    this.model.setProperty("/targetAttribute", targetAttribute);
    this.mappingTableSchemaFieldSelector.preselectSchemaFieldSelector(targetAttribute);
  }

  prefillOutputColumn(outputColumn) {
    this.model.setProperty("/outputColumn", outputColumn);
  }

  onNewColumnSubmit() {
    const column = {
      "targetAttribute" : this.model.getProperty("/targetAttribute"),
      "outputColumn" : this.model.getProperty("/outputColumn")
    };

    if (this.isValid(column)) {
      const outputColumnsPath = "/newRule/newOutputColumns";

      if (model.getProperty(outputColumnsPath) === undefined) {
        model.setProperty(outputColumnsPath, []);
      }
      const outputColumns = model.getProperty(outputColumnsPath);
      if (this.editIndex === undefined) {
        outputColumns.push(column);
      } else {
        outputColumns[this.editIndex] = column;
        this.editIndex = undefined;
      }
      this.model.setProperty(outputColumnsPath, outputColumns);
      this.onNewColumnCancel(); // close & clean up
    }
  }

  isValid({outputColumn, targetAttribute}) {
    let isValid = true;

    const isOutputColumnEmpty = GenericService.isEmpty(outputColumn);
    const isTargetAttributeEmpty = GenericService.isEmpty(targetAttribute);
    const schemas = this.oController._transitiveSchemas;
    const rule = model.getProperty("/newRule");

    if (isOutputColumnEmpty && isTargetAttributeEmpty) {
      sap.m.MessageToast.show("No target attribute or output column field selected.");
      isValid = false;
    } else if (isOutputColumnEmpty) {
      sap.m.MessageToast.show("No output column filled.");
      isValid = false;
    } else if (isTargetAttributeEmpty) {
      this.mappingTableSchemaFieldSelector.setErrorHighlight();
      sap.m.MessageToast.show("No target attribute(mapping table field) selected.");
      isValid = false;
    } else if (outputColumn.includes(".")){
      const isPathToStruct = SchemaManager.validatePathOfStructs(outputColumn,  schemas[rule.order].fields);
      if (!isPathToStruct.isValid) {
        sap.m.MessageToast.show(isPathToStruct.error);
        isValid = false;
      }
    } else {
      const hasNameClases = SchemaManager.validateColumnNameAvailable(outputColumn, schemas, rule.order);
      if (!hasNameClases.isValid) {
        sap.m.MessageToast.show(hasNameClases.error);
        isValid = false;
      }

    }

    return isValid;
  }

  onNewColumnCancel() {
    this.oDialog.close();
    this.reset()
  }

  onAddPress() {
    this.oDialog.setBusy(true);
    this.oDialog.open();
    this.oDialog.setBusy(false);
  }

  onEditPress(index, targetAttribute, outputColumn) {
    this.oDialog.setBusy(true);
    this.editIndex = index;
    this.oDialog.open();
    this.prefillOutputColumn(outputColumn);
    this.preselectMappingTableField(targetAttribute);
    this.oDialog.setBusy(false);
  }

}
