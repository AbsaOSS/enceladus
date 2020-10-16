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

class SchemaTable {

  constructor(oController, fragmentId) {
    this._oController = oController;
    this._parentId = fragmentId;

    this._schemaTable = oController.byId(sap.ui.core.Fragment.createId(this._parentId, "schemaFieldsTreeTable"));
    oController.byId(sap.ui.core.Fragment.createId(this._parentId, "metadataButton")).attachPress(this.metadataPress, this); // worked with "schemaFragment--metadataButton"

    this._oPopoverTemplate = new sap.m.List({})
    this._oPopover = new sap.m.Popover({
      title: "Metadata",
      content: [this._oPopoverTemplate],
      placement: sap.m.PlacementType.Left
    });
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
    const bOpen = this._oPopover.isOpen();
    const oSrc = oEv.getSource();
    if(bOpen) {
      this._oPopover.close();
    }
    if(oSrc !== this._lastMetatadaEvSrc) {
      let binding = oSrc.getBindingContext("schema").getPath() + "/metadata";
      let bindingArr = binding + "Arr";

      const model = this.oController.byId(sap.ui.core.Fragment.createId(this._parentId, "schemaFieldsTreeTable")).getModel("schema");
      let arrMeta = Formatters.objToKVArray(model.getProperty(binding));
      model.setProperty(bindingArr, arrMeta);
      this._oPopoverTemplate.setModel(model);
      this._oPopoverTemplate.bindItems({
        path: bindingArr,
        template: new sap.m.StandardListItem({
          title: "{key}",
          info: "{value}"
        })
      });
      this._lastMetatadaEvSrc = oSrc;
      this._oPopover.openBy(oSrc);
    } else if(!bOpen) {
      this._oPopover.openBy(oSrc);
    }
  }
}
