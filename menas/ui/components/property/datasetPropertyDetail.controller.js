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
  "sap/m/MessageToast",
  "sap/m/MessageItem",
  "sap/m/MessageBox",
  "components/AuditTrail"
], function (Controller, Fragment, MessageToast, MessageItem, MessageBox, AuditTrail) {
  "use strict";

  return Controller.extend("components.property.datasetPropertyDetail", {

    onInit: function () {
      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("properties").attachMatched(function (oEvent) {
        let args = oEvent.getParameter("arguments");
        this.routeMatched(args);
      }, this);

      this._eventBus = sap.ui.getCore().getEventBus();
      // this._eventBus.subscribe("properties", "updated", this.onEntityUpdated, this);

      this._datasetPropertiesService = new DatasetPropertiesService(this._model, this._eventBus);

      this._model.setProperty("/currentProperty", "");
    },


    routeMatched: function (oParams) {
      if (Prop.get(oParams, "id") === undefined) {
        this._datasetPropertiesService.getTop().then(() => this.load())
      } {
        this._datasetPropertiesService.getPropertyDefinition(oParams.id).then(() => this.load())
      }
      this.byId("propertyIconTabBar").setSelectedKey("info");
    },

    /*usedInNavTo: function (oEv) {
      let source = oEv.getSource();
      sap.ui.core.UIComponent.getRouterFor(this).navTo(source.data("collection"), {
        id: source.data("name"),
        version: source.data("version")
      });
    },*/

    load: function () {
      const currentProperty = this._model.getProperty("/currentProperty");
      this.byId("info").setModel(new sap.ui.model.json.JSONModel(currentProperty), "property");
      if (currentProperty) {
        this._datasetPropertiesService.getPropertyDefinition()
          .then(version => this._model.setProperty("/editingEnabled", currentProperty.version === version));
      }
    }

  });
});
