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

var AddMappingTableFragment = function (oController, fnLoad) {

  let loadDialogFragment = () => {
    let oView = oController.getView();

    fnLoad({
      id: oView.getId(),
      name: "components.mappingTable.addMappingTable",
      controller: oController
    }).then(function (oDialog) {
      oView.addDependent(oDialog);
    });

    return oController.byId("addMappingTableDialog");
  };

  let oDialog = loadDialogFragment();

  const model = sap.ui.getCore().getModel();
  const eventBus = sap.ui.getCore().getEventBus();
  const mappingTableService = new MappingTableService(model, eventBus);
  const schemaService = new SchemaService(model, eventBus);

  let oFragment = {
    submit: function () {
      let oMT = oDialog.getModel("entity").oData;
      // we may want to wait for a call to determine whether this is unique
      if (!oMT.isEdit && oMT.name && typeof (oMT.nameUnique) === "undefined") {
        // need to wait for the service call
        setTimeout(this.submit.bind(this), 500);
        return;
      }

      if (this.isValid(oMT)) {
        // send and update UI
        if (oMT.isEdit) {
          mappingTableService.update(oMT);
        } else {
          mappingTableService.create(oMT);
        }
        this.cancel(); // close & clean up
      }
    },

    cancel: function () {
      // This is a workaround for a bug in the Tree component of 1.56.5
      // TODO: verify whether this was fixed in the subsequent versions
      let tree = oController.byId("addMtHDFSBrowser");
      tree.unselectAll();

      tree.collapseAll();
      this.resetValueState();
      oDialog.close();
    },

    resetValueState: function () {
      oController.byId("newMappingTableName").setValueState(sap.ui.core.ValueState.None);
      oController.byId("newMappingTableName").setValueStateText("");

      oController.byId("schemaNameSelect").setValueState(sap.ui.core.ValueState.None);
      oController.byId("schemaNameSelect").setValueStateText("");

      oController.byId("schemaVersionSelect").setValueState(sap.ui.core.ValueState.None);
      oController.byId("schemaVersionSelect").setValueStateText("");
    },

    isValid: function (oMT) {
      this.resetValueState();

      let hasValidName = EntityValidationService.hasValidName(oMT, "Mapping Table",
        oController.byId("newMappingTableName"));
      let hasValidSchema = EntityValidationService.hasValidSchema(oMT, "Mapping Table",
        oController.byId("schemaNameSelect"), oController.byId("schemaVersionSelect"));
      let hasValidHDFSPath = oController.byId("addMtHDFSBrowser").validate();

      return hasValidName && hasValidSchema && hasValidHDFSPath;
    },

    onNameChange: function () {
      let sName = oDialog.getModel("entity").getProperty("/name");
      if (GenericService.isValidEntityName(sName)) {
        MappingTableService.hasUniqueName(sName, oDialog.getModel("entity"))
      } else {
        oDialog.getModel("entity").setProperty("/nameUnique", true);
      }
    },

    onSchemaSelect: function (oEv) {
      let sSchemaId = oEv.getParameter("selectedItem").getKey();
      schemaService.getAllVersions(sSchemaId, oController.byId("schemaVersionSelect"),
        oDialog.getModel("entity"), "/schemaVersion")
    }
  };

  this.getAdd = function() {
    oFragment.onPress = () => {

      oDialog.setBusy(true);
      oDialog.open();

      schemaService.getList(oDialog).then(oData => {
        const oFirstSchema = oData[0];
        schemaService.getAllVersions(oFirstSchema._id);

        oDialog.setModel(new sap.ui.model.json.JSONModel({
          name: "",
          description: "",
          schemaName: oFirstSchema._id,
          schemaVersion: oFirstSchema.latestVersion,
          hdfsPath: "/",
          isEdit: false,
          title: "Add"
        }), "entity");

        oDialog.setBusy(false);
      });
    };

    return oFragment;
  };

  this.getEdit = function() {
    oFragment.onPress = () => {

      oDialog.setBusy(true);
      oDialog.open();

      schemaService.getList(oDialog).then(() => {
        const current = oController._model.getProperty("/currentMappingTable");

        current.isEdit = true;
        current.title = "Edit";
        schemaService.getAllVersions(current.schemaName, oController.byId("schemaVersionSelect"));

        oDialog.setModel(new sap.ui.model.json.JSONModel(jQuery.extend(true, {}, current)), "entity");
        oDialog.setBusy(false);
      });
    };

    return oFragment;
  };

};
