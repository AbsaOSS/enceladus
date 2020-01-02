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
  "sap/m/MessageToast",
  "sap/base/i18n/ResourceBundle"
], function (Controller, Fragment, MessageToast, ResourceBundle) {
  "use strict";

  return Controller.extend("components.app", {

    onInit: function (oEv) {
      this._eventBus = sap.ui.getCore().getEventBus();
      this._eventBus.subscribe("nav", "back", this.onPressMasterBack, this);
      this._eventBus.subscribe("nav", "logout", this.onLogout, this);
      this._eventBus.subscribe("schemas", "created", this.onEntityCreated, this);
      this._eventBus.subscribe("datasets", "created", this.onEntityCreated, this);
      this._eventBus.subscribe("mappingTables", "created", this.onEntityCreated, this);
      this._eventBus.subscribe("runs", "dataset-name-selected", this.onRunsDatasetNamePress, this);
      this._eventBus.subscribe("runs", "dataset-version-selected", this.onRunsDatasetVersionPress, this);

      this._app = this.byId("menasApp");

      // register TNT icons
      sap.ui.core.IconPool.registerFont({
        collectionName: "SAP-icons-TNT",
        fontFamily: "SAP-icons-TNT",
        fontURI: sap.ui.require.toUrl("sap/tnt/themes/base/fonts"),
        lazy: true});

      this._model = sap.ui.getCore().getModel();

      this.getView().setModel(sap.ui.getCore().getModel());
      this._router = sap.ui.core.UIComponent.getRouterFor(this);

      GenericService.getUserInfo();
      GenericService.getOozieInfo();

      this._router.getRoute("root").attachMatched((oEvent) => {
        let userInfo = sap.ui.getCore().getModel().getProperty("/userInfo");
        if (typeof userInfo.username === 'undefined') {
          this._router.navTo("login");
        } else {
          this._router.navTo("home");
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

      //We want to override some of the default i18n texts
      const sapMRb = sap.ui.getCore().getLibraryResourceBundle("sap.m");
      const overrideRb = ResourceBundle.create({url: "components/i18n/message.properties"});
      const newRb = new sap.ui.model.resource.ResourceModel({
        bundle: sapMRb,
        enhanceWith: [overrideRb]
      });
      sap.ui.getCore().setModel(newRb, "i18n");

      window.addEventListener('resize', function(oEv) {
        this._eventBus.publish("menas", "resize", oEv);
      }.bind(this));

    },

    onPressMasterBack: function () {
      this._app.backMaster();
    },

    onRunsPress: function (oEv) {
      this._eventBus.publish("runs", "list-grouped-name");
      this._app.toMaster(this.createId("runsDatasetNamePage"));
    },

    onRunsDatasetNamePress: function (channel, event, data) {
      this._eventBus.publish("runs", "list-grouped-version", data);
      this._app.toMaster(this.createId("runsDatasetVersionPage"));
    },

    onRunsDatasetVersionPress: function (channel, event, dataset) {
      console.log(dataset)

      this._eventBus.publish("runs", "list", dataset);
      this._app.toMaster(this.createId("runsPage"));
    },

    onHomePress: function (oEv) {
      this._eventBus.publish("home", "home");
      this._router.navTo("home")
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
      this._router.navTo(sTopic, {
        id: oData.name,
        version: oData.version
      })
    },

    onLogout: function () {
      this._router.navTo("root");
    }

  });
});
