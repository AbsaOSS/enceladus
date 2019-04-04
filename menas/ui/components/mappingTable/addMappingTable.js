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

  let oFragment = {
    submit: function () {
      let oMT = oController._model.getProperty("/newMappingTable");
      // we may want to wait for a call to determine whether this is unique
      if (!oMT.isEdit && oMT.name && typeof (oMT.nameUnique) === "undefined") {
        // need to wait for the service call
        setTimeout(this.submit.bind(this), 500);
        return;
      }

      if (this.isValid(oMT)) {
        // send and update UI
        if (oMT.isEdit) {
          MappingTableService.editMappingTable(oMT.name, oMT.version, oMT.description, oMT.hdfsPath, oMT.schemaName, oMT.schemaVersion);
        } else {
          MappingTableService.createMappingTable(oMT.name, oMT.description, oMT.hdfsPath, oMT.schemaName, oMT.schemaVersion);
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

      oController.byId("newMappingTableSchemaNameSelect").setValueState(sap.ui.core.ValueState.None);
      oController.byId("newMappingTableSchemaNameSelect").setValueStateText("");

      oController.byId("newMappingTableSchemaVersionSelect").setValueState(sap.ui.core.ValueState.None);
      oController.byId("newMappingTableSchemaVersionSelect").setValueStateText("");

      oController.byId("addMtHDFSBrowser").setValueState(sap.ui.core.ValueState.None);
    },

    isValid: function (oMT) {
      this.resetValueState();

      let hasValidName = EntityValidationService.hasValidName(oMT, "Mapping Table",
        oController.byId("newMappingTableName"));
      let hasValidSchema = EntityValidationService.hasValidSchema(oMT, "Dataset",
        oController.byId("newMappingTableSchemaNameSelect"), oController.byId("newMappingTableSchemaVersionSelect"));
      let hasValidHDFSPath = EntityValidationService.hasValidHDFSPath(oMT, "Mapping Table",
        oController.byId("addMtHDFSBrowser"));

      return hasValidName && hasValidSchema && hasValidHDFSPath;
    },

    onNameChange: function () {
      let sName = oController._model.getProperty("/newMappingTable/name");
      if (GenericService.isValidEntityName(sName)) {
        MappingTableService.hasUniqueName(sName)
      } else {
        oController._model.setProperty("/newMappingTable/nameUnique", true)
      }
    },

    onSchemaSelect: function (oEv) {
      let sSchemaId = oEv.getParameter("selectedItem").getKey();
      SchemaService.getAllSchemaVersions(sSchemaId, oController.byId("newMappingTableSchemaVersionSelect"),
        oController._model, "/newMappingTable/schemaVersion")
    }
  };

  this.getAdd = function() {
    oFragment.onPress = () => {
      SchemaService.getSchemaList(oDialog, oData => {
        const oFirstSchema = oData[0];
        SchemaService.getAllSchemaVersions(oFirstSchema._id);

        oController._model.setProperty("/newMappingTable", {
          name: "",
          description: "",
          schemaName: oFirstSchema._id,
          schemaVersion: oFirstSchema.latestVersion,
          hdfsPath: "/",
          isEdit: false,
          title: "Add"
        });

        oDialog.open();
      });
    };

    return oFragment;
  };

  this.getEdit = function() {
    oFragment.onPress = () => {
      SchemaService.getSchemaList(oDialog, () => {
        const current = oController._model.getProperty("/currentMappingTable");

        current.isEdit = true;
        current.title = "Edit";

        oController._model.setProperty("/newMappingTable", jQuery.extend(true, {}, current));

        SchemaService.getAllSchemaVersions(current.schemaName, oController.byId("newMappingTableSchemaVersionSelect"));

        oDialog.open();
      });
    };

    return oFragment;
  };

};
