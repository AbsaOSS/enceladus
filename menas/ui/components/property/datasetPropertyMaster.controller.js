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

sap.ui.define([
  "sap/ui/core/mvc/Controller",
  "sap/ui/core/Fragment",
  "components/schema/selector/SchemaSelectorUtils"
], function (Controller, Fragment) {
  "use strict";

  return Controller.extend("components.property.datasetPropertyMaster", {

    onInit: function () {
      this._eventBus = sap.ui.getCore().getEventBus();
      this._eventBus.subscribe("properties", "list", this.list, this);

      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);

      this._propertiesService = new DatasetPropertiesService(this._model, this._eventBus)
    },

    list: function () {
      this._propertiesService.getList(this.byId("masterPage"));
    },

    onPressMasterBack: function () {
      this._eventBus.publish("nav", "back");
    },

    propertySelected: function (oEv) {
      let selected = oEv.getParameter("listItem").getTitle();
      this._router.navTo("properties", {
        id: selected
      });
    }

  });
});
