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
  "sap/m/MessageItem",
  "sap/m/MessageBox",
  "sap/m/MessagePopover"
], function (Controller, Fragment, MessageToast, MessageItem, MessageBox, MessagePopover) {
  "use strict";

  return Controller.extend("components.schema.schemaDetail", {

    onInit: function () {
      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("schemas").attachMatched(function (oEvent) {
        let args = oEvent.getParameter("arguments");
        this.routeMatched(args);
      }, this);

      // include CSRF token
      this.byId("fileUploader").addHeaderParameter(new sap.ui.unified.FileUploaderParameter({
        name: "X-CSRF-TOKEN",
        value: localStorage.getItem("csrfToken")
      }));

      this._editFragment = new AddSchemaFragment(this, Fragment.load).getEdit();

      this._eventBus = sap.ui.getCore().getEventBus();
    },

    onEditPress: function () {
      this._editFragment.onPress();
    },

    onSchemaSubmit: function () {
      this._editFragment.submit();
    },

    onSchemaCancel: function () {
      this._editFragment.cancel();
    },

    usedInNavTo: function (oEv) {
      let source = oEv.getSource();
      sap.ui.core.UIComponent.getRouterFor(this).navTo(source.data("collection"), {
        id: source.data("name"),
        version: source.data("version")
      });
    },

    auditVersionPress: function (oEv) {
      let oSrc = oEv.getParameter("listItem");
      let oRef = oSrc.data("menasRef");
      this._router.navTo("schemas", {
        id: oRef.name,
        version: oRef.version
      });
    },

    metadataPress: function (oEv) {
      let binding = oEv.getSource().getBindingContext().getPath() + "/metadata";
      let bindingArr = binding + "Arr";
      //hmm bindAggregation doesn't take formatter :-/
      let arrMeta = Formatters.objToKVArray(this._model.getProperty(binding));
      this._model.setProperty(bindingArr, arrMeta);

      let oMessageTemplate = new MessageItem({
        title: '{key}',
        subtitle: '{value}',
        type: sap.ui.core.MessageType.None
      });

      let oMessagePopover = new MessagePopover({
        items: {
          path: bindingArr,
          template: oMessageTemplate
        }
      }).setModel(this._model);

      oMessagePopover.toggle(oEv.getSource());
    },

    onRemovePress: function (oEv) {
      let current = this._model.getProperty("/currentSchema");

      MessageBox.show("This action will remove all versions of the schema definition. \nAre you sure?", {
        icon: MessageBox.Icon.WARNING,
        title: "Are you sure?",
        actions: [MessageBox.Action.YES, MessageBox.Action.NO],
        onClose: function (oAction) {
          if (oAction === "YES") {
            SchemaService.disableSchema(current.name);
          }
        }
      });
    },

    routeMatched: function (oParams) {
      if (Prop.get(oParams, "id") === undefined) {
        SchemaService.getFirstSchema();
      } else if (Prop.get(oParams, "version") === undefined) {
        SchemaService.getLatestSchemaVersion(oParams.id);
      } else {
        SchemaService.getSchemaVersion(oParams.id, oParams.version)
      }
      this.byId("schemaIconTabBar").setSelectedKey("info");
    },

    handleUploadPress: function (oParams) {
      if (this.validateSchemaFileUplaod()) {
        sap.ui.core.BusyIndicator.show();
        this.byId("fileUploader").upload();
      }
    },

    handleUploadProgress: function (oParams) {
      console.log((oParams.getParameter("loaded") / oParams.getParameter("total")) * 100)
    },

    handleUploadComplete: function (oParams) {
      sap.ui.core.BusyIndicator.hide();
      let status = oParams.getParameter("status");

      if (status === 500) {
        MessageBox
          .error("Failed to upload new schema. Ensure that the file is a valid Spark-SQL JSON schema and try again.")
      } else if (status === 201) {
        MessageToast.show("Schema successfully uploaded.");
        let oData = JSON.parse(oParams.getParameter("responseRaw"));
        model.setProperty("/currentSchema", oData);
        // update the list as well - may be cheaper in future to update locally
        this._eventBus.publish("schemas", "list");
        // nav back to info
        this.byId("schemaIconTabBar").setSelectedKey("info");
      }
    },

    validateSchemaFileUplaod: function () {
      let isOk = true;
      let oSchemaFileUpload = this.byId("fileUploader").getValue();
      if (oSchemaFileUpload === "") {
        this.byId("fileUploader").setValueState(sap.ui.core.ValueState.Error);
        this.byId("fileUploader").setValueStateText("File Name cannot be empty, Please select a file");
        isOk = false;
      }
      return isOk;
    }

  });
});
