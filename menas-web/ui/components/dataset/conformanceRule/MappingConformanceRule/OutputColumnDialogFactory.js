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

class OutputColumnDialogFactory {

  constructor(oController, fnLoad) {
    this._oController = oController;
    let oView = oController.getView();

    fnLoad({
      id: oView.getId(),
      name: "components.dataset.conformanceRule.MappingConformanceRule.outputColumnDialog",
      controller: oController
    }).then(function (oDialog) {
      oView.addDependent(oDialog);

      jQuery.sap.require("components.tables.TableUtils");

      const mtSchemaSelector = oController.byId("targetAttributeSelector");

      if(mtSchemaSelector) {
        const schemaFieldTableUtils = new components.tables.TableUtils(mtSchemaSelector, "");
        schemaFieldTableUtils.makeSearchable(["name", "absolutePath"]);
      } else {
        console.log(`No schema field selector matching mappingTableSchemaFieldSelector, skipping search initialization.`);
      }
    });

    this._oDialog = oController.byId("outputColumnDialog");
  }

  get oController() {
    return this._oController;
  }

  get oDialog() {
    return this._oDialog;
  }

  getDialog() {
    return new OutputColumnDialog(this.oDialog, this.oController, this.oController._model);
  }

}
