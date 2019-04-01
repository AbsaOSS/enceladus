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

var AddDatasetFragment = function (oController, fnLoad) {

  let loadDialogFragment = () => {
    let oView = oController.getView();

    fnLoad({
      id: oView.getId(),
      name: "components.dataset.addDataset",
      controller: oController
    }).then(function (oDialog) {
      oView.addDependent(oDialog);
    });

    return oController.byId("addDatasetDialog");
  };

  let oDialog = loadDialogFragment();

  let oFragment = {
    submit: function () {
      let oDataset = oDialog.getModel("entity").oData;
      // we may want to wait for a call to determine whether this is unique
      if (!oDataset.isEdit && oDataset.name && typeof (oDataset.nameUnique) === "undefined") {
        // need to wait for the service call
        setTimeout(this.submit.bind(this), 500);
        return;
      }

      if (this.isValid(oDataset)) {
        // send and update UI
        if (oDataset.isEdit) {
          DatasetService.editDataset(oDataset)
        } else {
          DatasetService.createDataset(oDataset)
        }
        this.cancel(); // close & clean up
      }
    },

    cancel: function () {
      let tree = oController.byId("newDatasetRawHDFSBrowser");
      tree.unselectAll();
      tree.collapseAll();

      let treePublish = oController.byId("newDatasetPublishHDFSBrowser");
      treePublish.unselectAll();
      treePublish.collapseAll();

      this.resetValueState();
      oDialog.close();
    },

    resetValueState: function () {
      oController.byId("newDatasetName").setValueState(sap.ui.core.ValueState.None);
      oController.byId("newDatasetName").setValueStateText("");

      oController.byId("schemaNameSelect").setValueState(sap.ui.core.ValueState.None);
      oController.byId("schemaNameSelect").setValueStateText("");

      oController.byId("schemaVersionSelect").setValueState(sap.ui.core.ValueState.None);
      oController.byId("schemaVersionSelect").setValueStateText("");

      oController.byId("newDatasetRawHDFSBrowser").setValueState(sap.ui.core.ValueState.None);
      oController.byId("newDatasetPublishHDFSBrowser").setValueState(sap.ui.core.ValueState.None);
    },

    isValid: function (oDataset) {
      this.resetValueState();

      let hasValidName = EntityValidationService.hasValidName(oDataset, "Dataset",
        oController.byId("newDatasetName"));
      let hasValidSchema = EntityValidationService.hasValidSchema(oDataset, "Dataset",
        oController.byId("schemaNameSelect"), oController.byId("schemaVersionSelect"));
      let hasValidHDFSPaths = EntityValidationService.hasValidHDFSPaths(oDataset, "Dataset",
        oController.byId("newDatasetRawHDFSBrowser"), oController.byId("newDatasetPublishHDFSBrowser"));

      return hasValidName && hasValidSchema && hasValidHDFSPaths;
    },

    onNameChange: function () {
      let sName = oDialog.getModel("entity").getProperty("/name");
      if (GenericService.isValidEntityName(sName)) {
        DatasetService.hasUniqueName(sName, oDialog.getModel("entity"))
      } else {
        oDialog.getModel("entity").setProperty("/nameUnique", true);
      }
    },

    onSchemaSelect: function (oEv) {
      let sSchemaId = oEv.getParameter("selectedItem").getKey();
      SchemaService.getAllSchemaVersions(sSchemaId, sap.ui.getCore().byId("schemaVersionSelect"),
        oDialog.getModel("entity"), "/schemaVersion")
    }
  };

  this.getAdd = function() {
    oFragment.onPress = () => {
      let oFirstSchema = oController._model.getProperty("/schemas")[0];

      oDialog.setModel(new sap.ui.model.json.JSONModel({
        name: "",
        description: "",
        schemaName: oFirstSchema._id,
        schemaVersion: oFirstSchema.latestVersion,
        hdfsPath: "/",
        hdfsPublishPath: "/",
        isEdit: false,
        title: "Add"
      }), "entity");

      oDialog.open();
    };

    return oFragment;
  };

  this.getEdit = function() {
    oFragment.onPress = () => {
      let current = oController._model.getProperty("/currentDataset");
      current.isEdit = true;
      current.title = "Edit";

      oDialog.setModel(new sap.ui.model.json.JSONModel(jQuery.extend(true, {}, current)), "entity");

      SchemaService.getAllSchemaVersions(current.schemaName, sap.ui.getCore().byId("schemaVersionSelect"));

      oDialog.open();
    };

    return oFragment;
  };

};
