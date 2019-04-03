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
  "sap/ui/core/Fragment",
  "sap/m/MessageToast"
], function (Controller, Fragment, MessageToast) {
  "use strict";

  return Controller.extend("components.app", {

    onInit: function (oEv) {
      this._eventBus = sap.ui.getCore().getEventBus();
      this._eventBus.subscribe("nav", "back", this.onPressMasterBack, this);
      this._eventBus.subscribe("nav", "logout", this.onLogout, this);
      this._eventBus.subscribe("schemas", "created", this.onEntityCreated, this);
      this._eventBus.subscribe("datasets", "created", this.onEntityCreated, this);
      this._eventBus.subscribe("mappingTables", "created", this.onEntityCreated, this);

      this._app = this.byId("menasApp");

      this._model = sap.ui.getCore().getModel();

      this.getView().setModel(sap.ui.getCore().getModel());
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      GenericService.getUserInfo(this);

      this._router.getRoute("root").attachMatched((oEvent) => {
        let userInfo = sap.ui.getCore().getModel().getProperty("/userInfo");
        if (typeof userInfo.username === 'undefined') {
          this._router.navTo("login");
        } else {
          this._router.navTo("runs");
        }
      }, this);

      this._router.getRoute("login").attachMatched((oEvent) => {
        let userInfo = sap.ui.getCore().getModel().getProperty("/userInfo");
        if (typeof userInfo.username !== 'undefined') {
          this._router.navTo("root");
          MessageToast.show("You are already logged in as " + userInfo.username)
        }
      }, this);

      this._router.attachRouteMatched((oEvent) => {
        let userInfo = this._model.getProperty("/userInfo");
        if (typeof userInfo.username === 'undefined') {
          this._router.navTo("login");
        }
      });
    },

    handleMenuPress: function (oEv) {
      this._app.hideMaster();
    },

    onPressMasterBack: function () {
      this._app.backMaster();
    },

    onRunsPress: function (oEv) {
      this._eventBus.publish("runs", "list");
      this._app.toMaster(this.createId("runsPage"));
    },

    onSchemasPress: function (oEv) {
      this._eventBus.publish("schemas", "list");
      this._app.toMaster(this.createId("schemasPage"));
    },

    onDatasetPress: function (oEv) {
      this._eventBus.publish("datasets", "list");
      this._app.toMaster(this.createId("datasetsPage"));
    },

    onMappingPress: function (oEv) {
      this._eventBus.publish("mappingTables", "list");
      this._app.toMaster(this.createId("mappingTablesPage"));
    },

    onEntityCreated: function (sTopic, sEvent, oData) {
      this._eventBus.publish(sTopic, "list");
      this._router.navTo(sTopic, {
        id: oData.name,
        version: oData.version
      })
    },

    onLogout: function () {
      this._app.backToTopMaster();
      this._router.navTo("root");
    },

    onAfterRendering: function () {
      component.setBusy(false)
    }

  });
});
