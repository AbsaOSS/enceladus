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

class ConcatenationColumnDialog {

  constructor(oDialog, oController, model) {
    this._oDialog = oDialog;
    this._oController = oController;
    oController.byId("concatSubmitButton").attachPress(this.onConcatSubmit, this);
    oController.byId("concatCancelButton").attachPress(this.onConcatCancel, this);
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
    this.model.setProperty("/concatColumn", "");
    this.schemaFieldSelector.reset(this.oDialog);
  }

  onSchemaFieldSelect(oEv) {
    this.schemaFieldSelector.onSchemaFieldSelect(oEv, "/concatColumn");
  }

  preselectField(field) {
    this.model.setProperty("/concatColumn", field);
    this.schemaFieldSelector.preselectSchemaFieldSelector(field);
  }

  setSchema(schema) {
    this.oDialog.setModel(new sap.ui.model.json.JSONModel(schema), "schema");
  }

  onConcatSubmit() {
    const concatenationColumn = this.model.getProperty("/concatColumn");

    if (this.isValid(concatenationColumn)) {
      const inputColumnsPath = "/newRule/inputColumns";
      if (model.getProperty(inputColumnsPath) === undefined) {
        model.setProperty(inputColumnsPath, [])
      }
      const inputColumns = model.getProperty(inputColumnsPath);
      if (this.editIndex === undefined) {
        inputColumns.push(concatenationColumn);
      } else {
        inputColumns[this.editIndex] = concatenationColumn;
        this.editIndex = undefined;
      }
      model.setProperty(inputColumnsPath, inputColumns);
      this.onConcatCancel(); // close & clean up
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

  onConcatCancel() {
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
