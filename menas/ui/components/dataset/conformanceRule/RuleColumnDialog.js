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

class RuleColumnDialog {

  constructor(oDialog, oController, model) {
    this._oDialog = oDialog;
    this._oController = oController;
    oController.byId("columnSubmitButton").attachPress(this.onColumnSubmit, this);
    oController.byId("columnCancelButton").attachPress(this.onColumnCancel, this);
    oController.byId("schemaFieldSelector").attachSelectionChange(this.onSchemaFieldSelect, this);
    this._schemaFieldSelector = new SimpleSchemaFieldSelector(oController, this.oDialog);
    this._model = model;
  }

  get oDialog() {
    return this._oDialog;
  }

  get model() {
    return this._model;
  }

  get schemaFieldSelector() {
    return this._schemaFieldSelector;
  }

  get editIndex() {
    return this._editIndex;
  }

  set editIndex(index) {
    this._editIndex = index;
  }

  reset() {
    this.model.setProperty("/ruleColumn", "");
    this.schemaFieldSelector.reset(this.oDialog);
  }

  onSchemaFieldSelect(oEv) {
    this.schemaFieldSelector.onSchemaFieldSelect(oEv, "/ruleColumn");
  }

  preselectField(field) {
    this.model.setProperty("/ruleColumn", field);
    this.schemaFieldSelector.preselectSchemaFieldSelector(field);
  }

  setSchema(schema) {
    const model = new sap.ui.model.json.JSONModel(schema);
    model.setSizeLimit(5000);
    this.oDialog.setModel(model, "schema");
  }

  onColumnSubmit() {
    const ruleColumn = this.model.getProperty("/ruleColumn");

    if (this.isValid(ruleColumn)) {
      const inputColumnsPath = "/newRule/inputColumns";
      if (model.getProperty(inputColumnsPath) === undefined) {
        model.setProperty(inputColumnsPath, [])
      }
      const inputColumns = model.getProperty(inputColumnsPath);
      if (this.editIndex === undefined) {
        inputColumns.push(ruleColumn);
      } else {
        inputColumns[this.editIndex] = ruleColumn;
        this.editIndex = undefined;
      }
      model.setProperty(inputColumnsPath, inputColumns);
      this.onColumnCancel(); // close & clean up
    }
  }

  isValid(fieldValue) {
    let isValid = true;

    if (GenericService.isEmpty(fieldValue)) {
      this.schemaFieldSelector.setErrorHighlight();
      sap.m.MessageToast.show("No field selected.");
      isValid = false;
    }

    return isValid;
  }

  onColumnCancel() {
    this.oDialog.close();
    this.reset();
  }

  onAddPress() {
    this.oDialog.setBusy(true);
    this.oDialog.open();
    this.oDialog.setBusy(false);
  }

  onEditPress(index, field) {
    this.oDialog.setBusy(true);
    this.editIndex = index;
    this.oDialog.open();
    this.preselectField(field);
    this.oDialog.setBusy(false);
  }

}
