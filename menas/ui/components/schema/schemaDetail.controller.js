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

  return Controller.extend("components.schema.schemaDetail", {

    onInit: function () {
      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("schemas").attachMatched(function (oEvent) {
        let args = oEvent.getParameter("arguments");
        this.routeMatched(args);
      }, this);

      new SchemaDialogFactory(this, Fragment.load).getEdit();

      this._eventBus = sap.ui.getCore().getEventBus();
      this._eventBus.subscribe("schemas", "updated", this.onEntityUpdated, this);

      this._schemaService = new SchemaService(this._model, this._eventBus);
      this._schemaTable = new SchemaTable(this);

      const auditTable = this.byId("auditTrailTable");
      const auditUtils = new AuditTrail(auditTable);
      auditUtils.applyTableUtils();

      this._model.setProperty("/topicNameStem", "");
      this._model.setProperty("/topicMergeWithKey", false);
    },

    onEntityUpdated: function (sTopic, sEvent, oData) {
      this._model.setProperty("/currentSchema", oData);
      this.load();
    },

    usedInNavTo: function (oEv) {
      let source = oEv.getSource();
      sap.ui.core.UIComponent.getRouterFor(this).navTo(source.data("collection"), {
        id: source.data("name"),
        version: source.data("version")
      });
    },

    auditVersionPress: function (oEv) {
      let oSrc = oEv.getSource();
      let oRef = oSrc.data("menasRef");
      this._router.navTo("schemas", {
        id: oRef.name,
        version: oRef.version
      });
    },

    onRemovePress: function (oEv) {
      let currentSchema = this._model.getProperty("/currentSchema");

      MessageBox.show("This action will remove all versions of the schema definition. \nAre you sure?", {
        icon: MessageBox.Icon.WARNING,
        title: "Are you sure?",
        actions: [MessageBox.Action.YES, MessageBox.Action.NO],
        onClose: (oAction) => {
          if (oAction === "YES") {
            this._schemaService.disable(currentSchema.name);
          }
        }
      });
    },

    onExportStructPress: function (oEv) {
      const schema = this._model.getProperty("/currentSchema");
      new SchemaRestDAO().getSchemaStruct(schema.name, schema.version)
        .then((data, status, request) => {
          const mimeType = "application/json";
          const extension = "-struct.json";
          this._downloadFile(schema, JSON.stringify(data, null, 2), mimeType, extension);
        })
        .fail(() => {
          sap.m.MessageToast.show(`No schema found for: "${schema.name}", version: ${schema.version}`)
        })
    },

    onExportFilePress: function (oEv) {
      const schema = this._model.getProperty("/currentSchema");
      new SchemaRestDAO().getSchemaFile(schema.name, schema.version)
        .then((data, status, request) => {
          const mimeType = request.getResponseHeader("mime-type");
          const extension = this._getFileExtension(mimeType);
          this._downloadFile(schema, data, mimeType, extension);
        })
        .fail(() => {
          sap.m.MessageToast.show(`No file uploaded for schema: "${schema.name}", version: ${schema.version}`)
        })
    },

    _downloadFile: function (schema, data, mimeType, extension) {
      const blob = new Blob([data], {type: mimeType});
      const filename = `${schema.name}-v${schema.version}${extension}`;
      if (window.navigator && window.navigator.msSaveOrOpenBlob) {
        window.navigator.msSaveOrOpenBlob(blob, filename);
      } else {
        const event = document.createEvent('MouseEvents');
        const element = document.createElement('a');

        element.download = filename;
        element.href = window.URL.createObjectURL(blob);
        element.dataset.downloadurl = [mimeType, element.download, element.href].join(':');
        event.initEvent('click', true, false);
        element.dispatchEvent(event);
      }
    },

    _getFileExtension: function (mimeType) {
      switch (mimeType) {
        case "application/json":
          return ".json";
        case "application/octet-stream":
          return ".cob"; //copybook
        case "application/vnd.schemaregistry.v1+json":
          return ".avsc";
        default:
          return "";
      }
    },

    routeMatched: function (oParams) {
      if (Prop.get(oParams, "id") === undefined) {
        this._schemaService.getTop().then(() => this.load())
      } else if (Prop.get(oParams, "version") === undefined) {
        this._schemaService.getLatestByName(oParams.id).then(() => this.load())
      } else {
        this._schemaService.getByNameAndVersion(oParams.id, oParams.version).then(() => this.load())
      }
      this.byId("schemaIconTabBar").setSelectedKey("info");
    },

    handleUploadPress: function (oParams) {
      if (this.validateSchemaFileUplaod()) {
        const oFileUpload = this.byId("fileUploader");
        sap.ui.core.BusyIndicator.show();
        // include CSRF token here - it may have changed between sessions
        oFileUpload.removeAllHeaderParameters();
        oFileUpload.addHeaderParameter(new sap.ui.unified.FileUploaderParameter({
          name: "X-CSRF-TOKEN",
          value: localStorage.getItem("csrfToken")
        }));
        oFileUpload.upload();
      }
    },

    handleTopicSubmit: function (oParams) {
        const schema = this._model.getProperty("/currentSchema");
        const topicStem = this._model.getProperty("/topicNameStem");
        const mergeWithKey = this._model.getProperty("/topicMergeWithKey");


        sap.ui.core.BusyIndicator.show();

        let data = {
          "format": "avro", // the only supported Schema registry type
          "topicStem": topicStem,
          "mergeWithKey": mergeWithKey,
          "name": schema.name,
          "version": schema.version
        };

        jQuery.ajax({
          url: "api/schema/topic",
          type: 'POST',
          data: $.param(data),
          contentType: 'application/x-www-form-urlencoded',
          context: this, // becomes the result of "this" in handleRemoteLoad*Complete
          headers: {
            'X-CSRF-TOKEN': localStorage.getItem("csrfToken")
          },
          complete: this.handleRemoteLoadFromTopicNameComplete
        });
    },

    handleRemoteUrlSubmit: function (oParams) {
      if (this.validateSchemaRemoteLoad()) { // marks errors or shows prompt if form not valid
        this.submitRemoteUrl();
      }
    },

    submitRemoteUrl: function () {
      const remoteUrl = this.byId("remoteUrl").getValue();
      const schema = this._model.getProperty("/currentSchema");

      sap.ui.core.BusyIndicator.show();

      let data = {
        "format": "avro", // the only supported Schema registry type
        "remoteUrl": remoteUrl,
        "name": schema.name,
        "version": schema.version
      };

      jQuery.ajax({
        url: "api/schema/remote",
        type: 'POST',
        data: $.param(data),
        contentType: 'application/x-www-form-urlencoded',
        context: this, // becomes the result of "this" in handleRemoteLoad*Complete
        headers: {
          'X-CSRF-TOKEN': localStorage.getItem("csrfToken")
        },
        complete: this.handleRemoteLoadFromUrlComplete
      });
    },

    validateSchemaRemoteLoad: function () {
      let isOkToSubmit = true;
      let schemaUrlInput = this.byId("remoteUrl");

      const schemaUrl = schemaUrlInput.getValue();
      if (!this.isHttpUrl(schemaUrl)) {
        schemaUrlInput.setValueState(sap.ui.core.ValueState.Error);
        schemaUrlInput.setValueStateText("The URL appear to be invalid. Please check it.");
        isOkToSubmit = false;

      } else if (this.isFixableSchemaRegistryUrl(schemaUrl)) {
        isOkToSubmit = false; // may get submitted by the MessageBox
        const fixedUrl = this.fixSchemaRegistryUrl(schemaUrl);

        sap.m.MessageBox.show(`The schema url should probably end with "/schema". \nDo you want use ${fixedUrl} instead?`, {
          icon: sap.m.MessageBox.Icon.WARNING,
          title: "Auto-correct schema repository URL?",
          actions: [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO, sap.m.MessageBox.Action.CANCEL],
          onClose: (oAction) => {
            if (oAction === sap.m.MessageBox.Action.YES) {
              schemaUrlInput.setValue(fixedUrl);
              this.submitRemoteUrl()
            } else if (oAction === sap.m.MessageBox.Action.NO) {
              this.submitRemoteUrl()
            } else {
              MessageToast.show(`Upload cancelled. Consider appending "/schema" to the URL`);
            }
          }
        });
      }

      return isOkToSubmit;
    },

    handleRemoteLoadFromTopicNameComplete: function (ajaxResponse) {
      this.handleRemoteLoadComplete(ajaxResponse, "topicName")
    },

    handleRemoteLoadFromUrlComplete: function (ajaxResponse) {
      this.handleRemoteLoadComplete(ajaxResponse, "remote")
    },

    // common for both loading from url and from topic name - differentiated by source param = "remote" | "topicName"
    handleRemoteLoadComplete: function (ajaxResponse, source) {
      // *very* similar as handleUploadComplete, but the response object is a bit different
      sap.ui.core.BusyIndicator.hide();
      let status = ajaxResponse.status;

      if (status === 201) {
        this.byId("remoteUrl").setValue("");
        model.setProperty("/topicNameStem", "");
        model.setProperty("/topicMergeWithKey", false);

        MessageToast.show("Schema successfully loaded.");
        let oData = JSON.parse(ajaxResponse.responseText);
        model.setProperty("/currentSchema", oData);
        this.load();
        // update the list as well - may be cheaper in future to update locally
        this._eventBus.publish("schemas", "list");
        // nav back to info
        this.byId("schemaIconTabBar").setSelectedKey("info");
      } else if (status === 400) {
        const errorMessage = ajaxResponse.responseJSON.message;
        const errorMessageDetails = errorMessage ? `\n\nDetails:\n${errorMessage}` : "";
        const errorType = ajaxResponse.responseJSON.error.errorType;

        let msg;
        if (errorType === "schema_retrieval_error") {
          if (source === "remote") {
            msg = `Error retrieving the schema file from "${this.byId("remoteUrl").getValue()}".${errorMessageDetails}`
          } else {
            msg = `Error retrieving the schema file by topic-name stem "${this.byId("topicNameBase").getValue()}".${errorMessageDetails}`
          }
        } else {
          msg = `Error parsing the schema file. Ensure that the file is a valid avro schema and ` +
            `try again.${errorMessageDetails}`
        }

        MessageBox.error(msg)
      } else if (status === 500) {
        MessageBox.error("Failed to load new schema. An internal server error has been occurred.")
      } else if (status === 401 || status === 403) {
        GenericService.clearSession("Session has expired");
      } else if (status === 0) {
        MessageBox.error(`Failed to load new schema. The connectivity to the server has been lost.`)
      } else {
        MessageBox.error(`Unexpected status=${status} error occurred. Please, check your connectivity to the server.`)
      }

      // clear error if present
      this.byId("remoteUrl").setValueState(sap.ui.core.ValueState.None);
      this.byId("remoteUrl").setValueStateText("");
    },

    handleUploadProgress: function (oParams) {
      console.log((oParams.getParameter("loaded") / oParams.getParameter("total")) * 100)
    },

    handleUploadComplete: function (oParams) {
      sap.ui.core.BusyIndicator.hide();
      let status = oParams.getParameter("status");

      if (status === 201) {
        this.byId("fileUploader").setValue(undefined);
        MessageToast.show("Schema successfully uploaded.");
        let oData = JSON.parse(oParams.getParameter("responseRaw"));
        model.setProperty("/currentSchema", oData);
        this.load();
        // update the list as well - may be cheaper in future to update locally
        this._eventBus.publish("schemas", "list");
        // nav back to info
        this.byId("schemaIconTabBar").setSelectedKey("info");
      } else if (status === 400) {
        const sSchemaType = this.byId("schemaFormatSelect").getSelectedItem().getText();
        const errorMessage = ResponseUtils.getBadRequestErrorMessage(oParams.getParameter("responseRaw"));
        const errorMessageDetails = errorMessage ? `\n\nDetails:\n${errorMessage}` : "";
        MessageBox.error(`Error parsing the schema file. Ensure that the file is a valid ${sSchemaType} schema and ` +
          `try again.${errorMessageDetails}`)
      } else if (status === 500) {
        MessageBox.error("Failed to upload new schema. An internal server error has been occurred.")
      } else if (status === 401 || status === 403) {
        GenericService.clearSession("Session has expired");
      } else if (status === 0) {
        MessageBox.error(`Failed to upload new schema. The connectivity to the server has been lost.`)
      } else {
        MessageBox.error(`Unexpected status=${status} error occurred. Please, check your connectivity to the server.`)
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
    },

    load: function () {
      const currentSchema = this._model.getProperty("/currentSchema");
      this.byId("info").setModel(new sap.ui.model.json.JSONModel(currentSchema), "schema");

      if (currentSchema) {
        this._schemaTable.model = this._model.getProperty("/currentSchema");
        const auditTable = this.byId("auditTrailTable");
        this._schemaService.getAuditTrail(currentSchema.name, auditTable);

        this._schemaRestDAO = new SchemaRestDAO();
        this._schemaRestDAO.getLatestVersionByName(currentSchema.name)
          .then(version => this._model.setProperty("/editingEnabled", currentSchema.version === version));
      }
    },

    httpUrlRx: new RegExp('^(https?:\\/\\/)?'+ // protocol
      '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|'+ // domain name
      'localhost|'+ // OR localhost
      '(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|'+ // OR IPv4 address
      '(([0-9a-f]{0,4}:){1,7}[0-9a-f]{0,4}))' + // OR IPv6 address (simplified version, allows ::1 and short other short forms)
      '(\\:\\d+)?' + // port
      '(\\/[-a-z\\d%_.~+]*)*'+ // path
      '(\\?[;&a-z\\d%_.~+=-]*)?'+ // query string
      '(\\#[-a-z\\d_]*)?$','i'), // fragment locator

    isHttpUrl: function (url) {
      if (!url) return false;
      return this.httpUrlRx.test(url);
    },

    // this is what the schema registry url should look like to be "fixable", e.g. http://example.schemaregistry.org/subjects/somename/versions/1
    schemaRegistryRx: /^(https?:\/\/[^ ]+\/versions\/(?:\d+|latest))\/?$/,

    isFixableSchemaRegistryUrl: function (str) {
      return this.schemaRegistryRx.test(str)
    },

    fixSchemaRegistryUrl: function (str) {
      // trailing slash gets stripped
      return str.replace(this.schemaRegistryRx, '$1/schema');
    },

    valueTopicFormatter: function(topicNameStem) {
      return `topic: ${topicNameStem}-value`;
    },

    valueKeyTopicFormatter: function(topicNameStem) {
      return `topics: ${topicNameStem}-value, ${topicNameStem}-key`;
    },

    topicFormatter: function(topicNameStem, includeKey) {
      if (includeKey)
        return this.valueKeyTopicFormatter(topicNameStem);
      else
        return this.valueTopicFormatter(topicNameStem);
    }

  });
});
