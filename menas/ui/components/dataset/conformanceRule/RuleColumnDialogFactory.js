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

class RuleColumnDialogFactory {

  constructor(oController, fnLoad) {
    this._oController = oController;
    let oView = oController.getView();

    fnLoad({
      id: oView.getId(),
      name: "components.dataset.conformanceRule.RuleColumnDialog",
      controller: oController
    }).then(function (oDialog) {
      oView.addDependent(oDialog);
    });

    this._oDialog = oController.byId("ruleColumnDialog");
  }

  get oController() {
    return this._oController;
  }

  get oDialog() {
    return this._oDialog;
  }

  getDialog() {
    return new RuleColumnDialog(this.oDialog, this.oController, this.oController._model);
  }

}
