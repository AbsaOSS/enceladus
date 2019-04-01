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

  return Controller.extend("components.schema.schemaMaster", {

    onInit: function () {
      this._eventBus = sap.ui.getCore().getEventBus();
      this._eventBus.subscribe("schemas", "list", this.list, this);

      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
    },

    list: function () {
      SchemaService.getSchemaList();
    },

    onPressMasterBack: function () {
      this._eventBus.publish("nav", "back");
    },

    schemaSelected: function (oEv) {
      let selected = oEv.getParameter("listItem").data("id");
      this._router.navTo("schemas", {
        id: selected
      });
    },

    onAddPress: function () {
      let oView = this.getView();

      let addSchemaDialog = this.byId("addSchemaDialog");
      if (!this.byId("addSchemaDialog")) {
        Fragment.load({
          id: oView.getId(),
          name: "components.schema.addSchema",
          controller: this
        }).then(function (oDialog) {
          oView.addDependent(oDialog);
          oDialog.open();
        });
      } else {
        addSchemaDialog.open()
      }
    },

    schemaAddSubmit: function () {
      let newSchema = this._model.getProperty("/newSchema");
      if (newSchema.name && typeof (newSchema.nameUnique) === "undefined") {
        // need to wait for the service call
        setTimeout(this.schemaAddSubmit.bind(this), 500);
      } else if (this.isValidSchema()) {
        // send and update UI
        SchemaService.createSchema(newSchema.name, newSchema.description);
        this.schemaAddCancel(); // close & clean up
      }
    },

    schemaAddCancel: function () {
      this.resetNewSchemaValueState();
      this._model.setProperty("/newSchema", {});
      this.byId("addSchemaDialog").close();
    },

    isValidSchema: function () {
      this.resetNewSchemaValueState();
      let schema = this._model.getProperty("/newSchema");

      let hasValidName = EntityValidationService.hasValidName(schema, "Schema",
        this.byId("newSchemaName"));

      return hasValidName;
    },

    resetNewSchemaValueState: function () {
      this.byId("newSchemaName").setValueState(sap.ui.core.ValueState.None);
      this.byId("newSchemaName").setValueStateText("");
    },

    schemaNameChange: function () {
      let sName = this._model.getProperty("/newSchema/name");
      if (GenericService.isValidEntityName(sName)) {
        SchemaService.hasUniqueName(sName)
      } else {
        this._model.setProperty("/newSchema/nameUnique", true)
      }
    }

  });
});
