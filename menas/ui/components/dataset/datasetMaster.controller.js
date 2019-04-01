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

sap.ui.define([
  "sap/ui/core/mvc/Controller",
  "sap/ui/core/Fragment"
], function (Controller, Fragment) {
  "use strict";

  return Controller.extend("components.dataset.datasetMaster", {

    onInit: function () {
      this._eventBus = sap.ui.getCore().getEventBus();
      this._eventBus.subscribe("dataset", "list", this.list, this);

      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);

      this._addFragment = new AddDatasetFragment(this, Fragment.load).getAdd();
    },

    list: function () {
      DatasetService.getDatasetList();
    },

    onPressMasterBack: function () {
      this._eventBus.publish("nav", "back");
    },

    datasetSelected: function (oEv) {
      let selected = oEv.getParameter("listItem").data("id");
      this._router.navTo("datasets", {
        id: selected
      });
    },

    onAddPress: function () {
      this._addFragment.onPress();
    },

    onDatasetSubmit: function () {
      this._addFragment.submit();
    },

    onDatasetCancel: function () {
      this._addFragment.cancel();
    },

    datasetNameChange: function () {
      this._addFragment.onNameChange();
    },

    onSchemaSelect: function (oEv) {
      this._addFragment.onSchemaSelect(oEv)
    },

    onAfterRendering: function () {
      // get schemas after rendering. This will be used for add/edit functionality
      let schemas = this._model.getProperty("/schemas");
      if (!schemas || schemas.length === 0) {
        SchemaService.getSchemaList(false, true);
      }
    }

  });
});
