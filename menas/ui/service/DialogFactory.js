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

class EntityDialogFactory {

  constructor(oController, fnLoad, fragmentName, dialogId) {
    this._oController = oController;
    let oView = oController.getView();

    fnLoad({
      id: oView.getId(),
      name: fragmentName,
      controller: oController
    }).then(function (oDialog) {
      oView.addDependent(oDialog);
    });

    this._oDialog = oController.byId(dialogId);
  }

  get oController() {
    return this._oController;
  }

  get oDialog() {
    return this._oDialog;
  }

}

class SchemaDialogFactory extends EntityDialogFactory {

  constructor(oController, fnLoad) {
    super(oController, fnLoad, "components.schema.addSchema", "addSchemaDialog")
    const model = sap.ui.getCore().getModel();
    const eventBus = sap.ui.getCore().getEventBus();
    this._schemaService = new SchemaService(model, eventBus);
  }

  get schemaService() {
    return this._schemaService;
  }

  getAdd() {
    const fragment = new AddSchemaDialog(this.oDialog, this.schemaService, this.oController);
    this.oController.byId("Add").attachPress(fragment.onPress, fragment);
    return fragment;
  }

  getEdit() {
    const fragment = new EditSchemaDialog(this.oDialog, this.schemaService, this.oController);
    this.oController.byId("Edit").attachPress(fragment.onPress, fragment);
    return fragment;
  }

}

class DatasetDialogFactory extends EntityDialogFactory {

  constructor(oController, fnLoad) {
    super(oController, fnLoad, "components.dataset.addDataset", "addDatasetDialog")
    const model = sap.ui.getCore().getModel();
    const eventBus = sap.ui.getCore().getEventBus();
    this._schemaService = new SchemaService(model, eventBus);
    this._datasetService = new DatasetService(model, eventBus);
  }

  get schemaService() {
    return this._schemaService;
  }

  get datasetService() {
    return this._datasetService;
  }

  getAdd() {
    const fragment = new AddDatasetDialog(this.oDialog, this.datasetService, this.schemaService, this.oController);
    this.oController.byId("Add").attachPress(fragment.onPress, fragment);
    return fragment;
  }

  getEdit() {
    const fragment = new EditDatasetDialog(this.oDialog, this.datasetService, this.schemaService, this.oController);
    this.oController.byId("Edit").attachPress(fragment.onPress, fragment);
    return fragment;
  }

}

class MappingTableDialogFactory extends EntityDialogFactory {

  constructor(oController, fnLoad) {
    super(oController, fnLoad, "components.mappingTable.addMappingTable", "addMappingTableDialog")
    const model = sap.ui.getCore().getModel();
    const eventBus = sap.ui.getCore().getEventBus();
    this._schemaService = new SchemaService(model, eventBus);
    this._mappingTableService = new MappingTableService(model, eventBus);
  }

  get schemaService() {
    return this._schemaService;
  }

  get mappingTableService() {
    return this._mappingTableService;
  }

  getAdd() {
    const fragment = new AddMappingTableDialog(this.oDialog, this.mappingTableService, this.schemaService, this.oController);
    this.oController.byId("Add").attachPress(fragment.onPress, fragment);
    return fragment;
  }

  getEdit() {
    const fragment = new EditMappingTableDialog(this.oDialog, this.mappingTableService, this.schemaService, this.oController);
    this.oController.byId("Edit").attachPress(fragment.onPress, fragment);
    return fragment;
  }

}

