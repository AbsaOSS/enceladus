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

class SchemaTable {

  constructor(oController) {
    this._oController = oController;
    this._schemaTable = oController.byId("schemaFieldsTreeTable");
    oController.byId("metadataButton").attachPress(this.metadataPress, this);
    this._oMessageTemplate = new sap.m.MessageItem({
      title: '{key}',
      subtitle: '{value}',
      type: sap.ui.core.MessageType.None
    });

    this._oMessagePopover = new sap.m.MessagePopover();
  }

  get schemaTable() {
    return this._schemaTable
  }

  get oController() {
    return this._oController
  }

  get model() {
    return this._model
  }

  set model(newModel) {
    newModel = new sap.ui.model.json.JSONModel(newModel);
    this.schemaTable.setModel(newModel, "schema");
    this._model = newModel;
  }

  metadataPress(oEv) {
    let binding = oEv.getSource().getBindingContext("schema").getPath() + "/metadata";
    let bindingArr = binding + "Arr";
    //hmm bindAggregation doesn't take formatter :-/
    const model = this.oController.byId("schemaFieldsTreeTable").getModel("schema");
    let arrMeta = Formatters.objToKVArray(model.getProperty(binding));
    model.setProperty(bindingArr, arrMeta);
    this._oMessagePopover.setModel(model);
    this._oMessagePopover.bindAggregation("items", {
      path: bindingArr,
      template: this._oMessageTemplate,
    });
    
    if(this._oMessagePopover.isOpen()) {
      this._oMessagePopover.close();
    }
    this._oMessagePopover.openBy(oEv.getSource());
  }

}
