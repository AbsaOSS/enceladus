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
jQuery.sap.require("sap.m.MessageBox");

sap.ui.controller("components.dataset.datasetMain", {

  /**
   * Called when a controller is instantiated and its View controls (if
   * available) are already created. Can be used to modify the View before it
   * is displayed, to bind event handlers and do other one-time
   * initialization.
   *
   * @memberOf components.dataset.datasetMain
   */
  onInit: function () {
    this._model = sap.ui.getCore().getModel();
    this._router = sap.ui.core.UIComponent.getRouterFor(this);
    this._router.getRoute("datasets").attachMatched(function (oEvent) {
      let arguments = oEvent.getParameter("arguments");
      this.routeMatched(arguments);
    }, this);

    this._addDialog = sap.ui.xmlfragment("components.dataset.addDataset", this);
    sap.ui.getCore().byId("newDatasetAddButton").attachPress(this.datasetAddSubmit, this);
    sap.ui.getCore().byId("newDatasetCancelButton").attachPress(this.datasetAddCancel, this);
    sap.ui.getCore().byId("newDatasetName").attachChange(this.datasetNameChange, this);
    sap.ui.getCore().byId("schemaNameSelect").attachChange(this.schemaSelect, this);
    this._addDialog.setModel(new sap.ui.model.json.JSONModel({
      isEdit: false,
      title: "Add"
    }), "entity");
    this._addDialog.setBusyIndicatorDelay(0);

    let cont = sap.ui.controller("components.dataset.conformanceRule.upsert", true);
    this._upsertConformanceRuleDialog = sap.ui.xmlfragment("components.dataset.conformanceRule.upsert", cont);
  },

  onAddConformanceRulePress: function () {
    this._model.setProperty("/newRule", {
      title: "Add",
      isEdit: false,
    });
    this.fetchSchema();
    this._upsertConformanceRuleDialog.open();
  },

  onRuleMenuAction: function (oEv) {
    let sAction = oEv.getParameter("item").data("action");
    let sBindPath = oEv.getParameter("item").getBindingContext().getPath();

    if (sAction === "edit") {
      let old = this._model.getProperty(sBindPath);
      this.fetchSchema();
      this._model.setProperty("/newRule", {
        ...JSON.parse(JSON.stringify(old)),
        title: "Edit",
        isEdit: true,
      });
      this._upsertConformanceRuleDialog.open();
    } else if (sAction === "delete") {
      sap.m.MessageBox.confirm("Are you sure you want to delete the conformance rule?", {
        actions: [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO],
        onClose: function (oResponse) {
          if (oResponse === sap.m.MessageBox.Action.YES) {
            let toks = sBindPath.split("/");
            let ruleIndex = parseInt(toks[toks.length - 1]);
            let currentDataset = this._model.getProperty("/currentDataset");
            let newDataset = RuleService.removeRule(currentDataset, ruleIndex);

            if (newDataset) {
              DatasetService.editDataset(newDataset);
            }
          }
        }.bind(this)
      });
    }
  },

  toSchema: function (oEv) {
    let src = oEv.getSource();
    sap.ui.core.UIComponent.getRouterFor(this).navTo("schemas", {
      id: src.data("name"),
      version: src.data("version")
    })
  },

  toMappingTable: function (oEv) {
    let src = oEv.getSource();
    sap.ui.core.UIComponent.getRouterFor(this).navTo("mappingTables", {
      id: src.data("name"),
      version: src.data("version")
    })
  },

  fetchSchema: function (oEv) {
    let dataset = sap.ui.getCore().getModel().getProperty("/currentDataset");
    if (typeof (dataset.schema) === "undefined") {
      SchemaService.getSchemaVersion(dataset.schemaName, dataset.schemaVersion, "/currentDataset/schema")
    }
  },

  datasetAddCancel: function () {
    let tree = sap.ui.getCore().byId("newDatasetRawHDFSBrowser");
    tree.unselectAll();
    tree.collapseAll();

    let treePublish = sap.ui.getCore().byId("newDatasetPublishHDFSBrowser");
    treePublish.unselectAll();
    treePublish.collapseAll();

    this.resetNewDatasetValueState();
    this._addDialog.close();
  },

  datasetAddSubmit: function () {
    let oDataset = this._addDialog.getModel("entity").oData;
    // we may wanna wait for a call to determine whether this is unique
    if (!oDataset.isEdit && oDataset.name && typeof (oDataset.nameUnique) === "undefined") {
      // need to wait for the service call
      setTimeout(this.datasetAddSubmit.bind(this), 500);
      return;
    }

    if (this.isValidDataset(oDataset)) {
      // send and update UI
      if (oDataset.isEdit) {
        DatasetService.editDataset(oDataset)
      } else {
        DatasetService.createDataset(oDataset)
      }
      this.datasetAddCancel(); // close & clean up
    }
  },

  resetNewDatasetValueState: function () {
    sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.None);
    sap.ui.getCore().byId("newDatasetName").setValueStateText("");

    sap.ui.getCore().byId("schemaNameSelect").setValueState(sap.ui.core.ValueState.None);
    sap.ui.getCore().byId("schemaNameSelect").setValueStateText("");

    sap.ui.getCore().byId("schemaVersionSelect").setValueState(sap.ui.core.ValueState.None);
    sap.ui.getCore().byId("schemaVersionSelect").setValueStateText("");

    sap.ui.getCore().byId("newDatasetRawHDFSBrowser").setValueState(sap.ui.core.ValueState.None);
    sap.ui.getCore().byId("newDatasetPublishHDFSBrowser").setValueState(sap.ui.core.ValueState.None);
  },

  schemaSelect: function (oEv) {
    let sSchemaId = oEv.getParameter("selectedItem").getKey();
    SchemaService.getAllSchemaVersions(sSchemaId, sap.ui.getCore().byId("schemaVersionSelect"))
  },

  mappingTableSelect: function (oEv) {
    let sMappingTableId = oEv.getParameter("selectedItem").getKey();
    MappingTableService.getAllMappingTableVersions(sMappingTableId, sap.ui.getCore().byId("schemaVersionSelect"))
  },

  tabSelect: function (oEv) {
    if (oEv.getParameter("selectedKey") === "schema")
      this.fetchSchema();
  },

  isValidDataset: function (oDataset) {
    this.resetNewDatasetValueState();
    let isOk = true;

    if (!oDataset.name || oDataset.name === "") {
      sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.Error);
      sap.ui.getCore().byId("newDatasetName").setValueStateText("Dataset name cannot be empty");
      isOk = false;
    } else {
      if (GenericService.hasWhitespace(oDataset.name)) {
        sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.Error);
        sap.ui.getCore().byId("newDatasetName").setValueStateText(
          "Dataset name '" + oDataset.name + "' should not have spaces. Please remove spaces and retry");
        isOk = false;
      }
      if (!oDataset.isEdit && !oDataset.nameUnique) {
        sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.Error);
        sap.ui.getCore().byId("newDatasetName").setValueStateText(
          "Dataset name '" + oDataset.name + "' already exists. Choose a different name.");
        isOk = false;
      }
    }
    if (!oDataset.schemaName || oDataset.schemaName === "") {
      sap.ui.getCore().byId("schemaNameSelect").setValueState(sap.ui.core.ValueState.Error);
      sap.ui.getCore().byId("schemaNameSelect").setValueStateText("Please choose the schema of the dataset");
      isOk = false;
    }
    if (oDataset.schemaVersion === undefined || oDataset.schemaVersion === "") {
      sap.ui.getCore().byId("schemaVersionSelect").setValueState(sap.ui.core.ValueState.Error);
      sap.ui.getCore().byId("schemaVersionSelect").setValueStateText("Please choose the version of the schema for the dataset");
      isOk = false;
    }
    if (!oDataset.hdfsPath || oDataset.hdfsPath === "") {
      sap.ui.getCore().byId("newDatasetRawHDFSBrowser").setValueState(sap.ui.core.ValueState.Error);
      sap.m.MessageToast.show("Please choose the raw HDFS path of the dataset");
      isOk = false;
    }
    if (!oDataset.hdfsPublishPath || oDataset.hdfsPublishPath === "") {
      sap.ui.getCore().byId("newDatasetPublishHDFSBrowser").setValueState(sap.ui.core.ValueState.Error);
      sap.m.MessageToast.show("Please choose the publish HDFS path of the dataset");
      isOk = false;
    }

    return isOk;
  },

  onAddPress: function () {
    let oFirstSchema = this._model.getProperty("/schemas")[0];

    this._addDialog.setModel(new sap.ui.model.json.JSONModel({
      name: "",
      description: "",
      schemaName: oFirstSchema._id,
      schemaVersion: oFirstSchema.latestVersion,
      hdfsPath: "/",
      hdfsPublishPath: "/",
      isEdit: false,
      title: "Add"
    }), "entity");

    this._addDialog.open();
  },

  datasetNameChange: function () {
    let sName = this._addDialog.getModel("entity").getProperty("/name");
    if (GenericService.isValidEntityName(sName)) {
      DatasetService.isNameUnique(sName, this._addDialog.getModel("entity"))
    }
  },

  onEditPress: function () {
    let current = this._model.getProperty("/currentDataset");
    current.isEdit = true;
    current.title = "Edit";

    this._addDialog.setModel(new sap.ui.model.json.JSONModel(current), "entity");

    SchemaService.getAllSchemaVersions(current.schemaName, sap.ui.getCore().byId("schemaVersionSelect"));

    this._addDialog.open();
  },

  onRemovePress: function (oEv) {
    let current = this._model.getProperty("/currentDataset");

    sap.m.MessageBox.show("This action will remove all versions of the dataset definition. \nAre you sure?.", {
      icon: sap.m.MessageBox.Icon.WARNING,
      title: "Are you sure?",
      actions: [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO],
      onClose: function (oAction) {
        if (oAction === "YES") {
          DatasetService.disableDataset(current.name)
        }
      }
    });
  },

  datasetSelected: function (oEv) {
    let selected = oEv.getParameter("listItem").data("id");
    this._router.navTo("datasets", {
      id: selected
    });
  },

  routeMatched: function (oParams) {
    if (Prop.get(oParams, "id") === undefined) {
      DatasetService.getDatasetList(true);
    } else if (Prop.get(oParams, "version") === undefined) {
      DatasetService.getDatasetList();
      DatasetService.getLatestDatasetVersion(oParams.id)
    } else {
      DatasetService.getDatasetList();
      DatasetService.getDatasetVersion(oParams.id, oParams.version)
    }
  },

  conformanceRuleFactory: function (sId, oContext) {
    let sFragmentName = "components.dataset.conformanceRule." + oContext.getProperty("_t") + ".display";
    if (oContext.getProperty("_t") === "MappingConformanceRule") {

      let oAttributeMappings = oContext.getProperty("attributeMappings");
      let aJoinConditions = [];
      for (let key in oAttributeMappings) {
        let mappingTableName = oContext.getProperty("mappingTable");
        let datasetName = this._model.getProperty("/currentDataset/name");
        aJoinConditions.push({
          mappingTableField: mappingTableName + "." + key,
          datasetField: datasetName + "." + oAttributeMappings[key]
        });
      }

      oContext.getObject().joinConditions = aJoinConditions;
    }

    return sap.ui.xmlfragment(sId, sFragmentName, this);
  },

  /**
   * Called when the View has been rendered (so its HTML is part of the document).
   * Post-rendering manipulations of the HTML could be done here. This hook is the
   * same one that SAPUI5 controls get after being rendered.
   *
   * @memberOf components.dataset.datasetMain
   */
  onAfterRendering: function () {
    // get schemas after rendering. This will be used for add/edit
    // functionality
    let schemas = this._model.getProperty("/schemas");
    if (!schemas || schemas.length === 0) {
      SchemaService.getSchemaList(false, true);
    }
  }

});
