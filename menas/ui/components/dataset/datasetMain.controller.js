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
jQuery.sap.require("sap.m.MessageBox");

sap.ui.controller("components.dataset.datasetMain", {

  rules: [
    {_t: "CastingConformanceRule"},
    {_t: "ConcatenationConformanceRule"},
    {_t: "DropConformanceRule"},
    {_t: "LiteralConformanceRule"},
    {_t: "MappingConformanceRule"},
    {_t: "NegationConformanceRule"},
    {_t: "SingleColumnConformanceRule"},
    {_t: "SparkSessionConfConformanceRule"},
    {_t: "UppercaseConformanceRule"}
  ],

  dataTypes: [
    {type: "boolean"},
    {type: "byte"},
    {type: "short"},
    {type: "integer"},
    {type: "long"},
    {type: "float"},
    {type: "double"},
    {type: "decimal(38,18)"},
    {type: "char"},
    {type: "string"},
    {type: "date"},
    {type: "timestamp"}
  ],

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

    this._addConformanceRuleDialog = sap.ui.xmlfragment("components.dataset.conformanceRule.add", this);
    sap.ui.getCore().byId("newRuleAddButton").attachPress(this.ruleAddSubmit, this);
    sap.ui.getCore().byId("newRuleCancelButton").attachPress(this.ruleAddCancel, this);
    sap.ui.getCore().byId("newRuleSelect").attachChange(this.ruleSelect, this);

    this._model.setProperty("/rules", this.rules);
    this._model.setProperty("/dataTypes", this.dataTypes);
    this._model.setProperty("/newRule", {});
  },

  onAddConformanceRulePress: function (oEv) {
    this._model.setProperty("/newRule", {
      // title: "Add", // TODO: add title
      _t: this.rules[0]._t
    });
    this.fetchSchema();
    this.showFormFragment(this.rules[0]._t);
    this._addConformanceRuleDialog.open();
  },

  ruleAddCancel: function () {
    this.resetRuleForm();
    this._addConformanceRuleDialog.close();
  },

  ruleAddSubmit: function () {
    // if (this.validateNewRule) {
    let currentDatasetName = this._model.getProperty("/currentDataset/name");
    let currentRule = this._model.getProperty("/newRule");
    RuleService.createRule(currentDatasetName, currentRule);
    // }
    this.ruleAddCancel();
  },

  schemaFieldSelect: function (oEv) {
    let bind = oEv.getParameter("listItem").getBindingContext().getPath();
    let modelPathBase = "/currentDataset/schema/fields/";
    let model = sap.ui.getCore().getModel();
    SchemaService.fieldSelect(bind, modelPathBase, model, "/newRule/inputColumn")
  },

  _formFragments: {},

  ruleSelect: function (oEv) {
    let currentRule = this._model.getProperty("/newRule");
    let newRule = (({_t, outputColumn, checkpoint: controlCheckpoint}) => ({
      _t,
      outputColumn,
      checkpoint: controlCheckpoint
    }))(currentRule);

    if (currentRule._t === "ConcatenationConformanceRule") {
      newRule.inputColumns = ["", ""];
    }

    if (currentRule._t === "MappingConformanceRule") {
      MappingTableService.getMappingTableList(true, true);
      newRule.joinConditions = [{mappingTableField: "", datasetField: ""}];
    }

    this._model.setProperty("/newRule", newRule);

    this.showFormFragment(currentRule._t);
  },

  getFormFragment: function (sFragmentName) {
    let oFormFragment = this._formFragments[sFragmentName];

    if (oFormFragment) {
      return oFormFragment;
    }
    const sFragmentId = this.getView().getId() + "--" + sFragmentName;
    oFormFragment = sap.ui.xmlfragment(sFragmentId, "components.dataset.conformanceRule." + sFragmentName + ".add", this);

    if (sFragmentName === "ConcatenationConformanceRule") {
      sap.ui.getCore().byId(sFragmentId + "--addInputColumn").attachPress(this.addInputColumn, this);
    }

    if (sFragmentName === "MappingConformanceRule") {
      sap.ui.getCore().byId(sFragmentId + "--mappingTableNameSelect").attachChange(this.mappingTableSelect, this);
      sap.ui.getCore().byId(sFragmentId + "--addJoinCondition").attachPress(this.addJoinCondition, this);
    }

    this._formFragments[sFragmentName] = oFormFragment;
    return this._formFragments[sFragmentName];
  },

  resetRuleForm: function () {
    let oRuleForm = sap.ui.getCore().byId("ruleForm");

    // workaround for "Cannot read property 'setSelectedIndex' of undefined" error
    const content = oRuleForm.getContent();
    content.filter(function (element) {
      return element.sId.includes("FieldSelectScroll")
    }).forEach(function (element) {
      element.getContent().forEach(function (tree) {
        let items = tree.getItems();
        for (let i in items) {
          items[i].setSelected(false);
          items[i].setHighlight(sap.ui.core.MessageType.None)
        }
      })
    });

    oRuleForm.removeAllContent();
  },

  showFormFragment: function (sFragmentName) {
    this.resetRuleForm();

    let oRuleForm = sap.ui.getCore().byId("ruleForm");
    let aFragment = this.getFormFragment(sFragmentName);
    aFragment.forEach(function (oElement) {
      oRuleForm.addContent(oElement)
    });
  },

  addInputColumn: function () {
    let currentRule = this._model.getProperty("/newRule");
    let inputColumnSize = currentRule.inputColumns.length;
    let pathToNewInputColumn = "/newRule/inputColumns/" + inputColumnSize;
    this._model.setProperty(pathToNewInputColumn, "");

    let oRuleForm = sap.ui.getCore().byId("ruleForm");
    oRuleForm.addContent(new sap.m.Label({text: "Concatenation Column"}));
    oRuleForm.addContent(new sap.m.Input({type: "Text", value: "{" + pathToNewInputColumn + "}"}))
  },

  addJoinCondition: function () {
    let currentRule = this._model.getProperty("/newRule");
    let joinConditionsSize = currentRule.joinConditions.length;
    let pathToNewJoinCondition = "/newRule/joinConditions/" + joinConditionsSize;
    this._model.setProperty(pathToNewJoinCondition, {mappingTableField: "", datasetField: ""});

    let oRuleForm = sap.ui.getCore().byId("ruleForm");
    let oHBox = new sap.m.HBox({justifyContent: "SpaceAround"});
    oHBox.addItem(new sap.m.Input({
      type: "Text",
      value: "{" + pathToNewJoinCondition + "/mappingTableField}",
      placeholder: "Mapping Table Column"
    }));
    oHBox.addItem(new sap.m.Input({
      type: "Text",
      value: "{" + pathToNewJoinCondition + "/datasetField}",
      placeholder: "Dataset Column"
    }));
    oRuleForm.addContent(new sap.m.Label({text: "Join Condition"}));
    oRuleForm.addContent(oHBox);
  },

  onRuleMenuAction: function (oEv) {
    let sAction = oEv.getParameter("item").data("action")
    let sBindPath = oEv.getParameter("item").getBindingContext().getPath();

    if (sAction === "edit") {
      let old = this._model.getProperty(sBindPath);
      old.title = "Edit";
      old.isEdit = true;
      old.bindPath = sBindPath;
      this._model.setProperty("/newRule", old);
      this._addConformanceRuleDialog.open();
      // this._schemaFieldSelectorSelectPath(old["columnName"])
    } else if (sAction === "delete") {
      sap.m.MessageBox.confirm("Are you sure you want to delete the conformance rule?", {
        actions: [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO],
        onClose: function (oResponse) {
          if (oResponse === sap.m.MessageBox.Action.YES) {
            let toks = sBindPath.split("/");
            let index = toks[toks.length - 1];
            let currDataset = this._model.getProperty("/currentDataset");
            let conformance = currDataset["conformance"].filter((el, ind) => ind !== parseInt(index));
            // RuleService.editConformanceRules(currDataset.name, currDataset.version, conformance);
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

    let treePublish = sap.ui.getCore().byId("newDatasetPublishDFSBrowser");
    treePublish.unselectAll();
    treePublish.collapseAll();

    this.resetNewDatasetValueState();
    this._addDialog.close();
  },

  datasetAddSubmit: function () {
    let newDataset = this._addDialog.getModel("entity").oData;

    // we may wanna wait for a call to determine whether this is unique
    if (!newDataset.isEdit && newDataset.name && typeof (newDataset.nameUnique) === "undefined") {
      // need to wait for the service call
      setTimeout(this.datasetAddSubmit.bind(this), 500);
      return;
    }
    if (this.validateNewDataset()) {
      // send and update UI
      if (newDataset.isEdit) {
        DatasetService.editDataset(newDataset.name, newDataset.version, newDataset.description, newDataset.hdfsPath, newDataset.hdfsPublishPath, newDataset.schemaName, newDataset.schemaVersion)
      } else {
        DatasetService.createDataset(newDataset.name, newDataset.description, newDataset.hdfsPath, newDataset.hdfsPublishPath, newDataset.schemaName, newDataset.schemaVersion)
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
    sap.ui.getCore().byId("newDatasetPublishDFSBrowser").setValueState(sap.ui.core.ValueState.None);
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

  _loadAllVersionsOfFirstSchema: function () {
    this._addDialog.setModel(new sap.ui.model.json.JSONModel({
      isEdit: false,
      title: "Add"
    }), "entity");

    let schemas = this._model.getProperty("/schemas");

    if (schemas.length > 0) {
      this._model.setProperty("/newSchema", {
        schemaName: schemas[0]._id
      });

      let sSchema = this._model.getProperty("/schemas/0/_id");
      SchemaService.getAllSchemaVersions(sSchema, sap.ui.getCore().byId("newDatsetSchemaVersionSelect"))
    }
  },

  validateNewDataset: function () {
    this.resetNewDatasetValueState();
    let oDataset = this._addDialog.getModel("entity").oData;
    let isOk = true;

    if (!oDataset.name || oDataset.name === "") {
      sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.Error);
      sap.ui.getCore().byId("newDatasetName").setValueStateText("Dataset name cannot be empty");
      isOk = false;
    }
    if (!oDataset.isEdit && !oDataset.nameUnique) {
      sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.Error);
      sap.ui.getCore().byId("newDatasetName").setValueStateText(
        "Dataset name '" + oDataset.name + "' already exists. Choose a different name.");
      isOk = false;
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
      sap.ui.getCore().byId("newDatasetRawHDFSBrowser").setValueState(sap.ui.core.ValueState.Error);
      sap.m.MessageToast.show("Please choose the publish HDFS path of the dataset");
      isOk = false;
    }

    return isOk;
  },

  onAddPress: function () {
    this._loadAllVersionsOfFirstSchema();
    this._addDialog.open();
  },

  datasetNameChange: function () {
    DatasetService.isUniqueDatasetName(this._addDialog.getModel("entity").getProperty("/name"), this._addDialog.getModel("entity"))
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
   * Similar to onAfterRendering, but this hook is invoked before the controller's
   * View is re-rendered (NOT before the first rendering! onInit() is used for
   * that one!).
   *
   * @memberOf components.dataset.datasetMain
   */
// onBeforeRendering: function() {
//
// },
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
  /**
   * Called when the Controller is destroyed. Use this one to free resources and
   * finalize activities.
   *
   * @memberOf components.dataset.datasetMain
   */
// onExit: function() {
//
// }
});
