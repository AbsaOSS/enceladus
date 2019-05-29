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

class ConformanceRuleDialog {

  constructor(controller) {
    this._model = sap.ui.getCore().getModel();
    this.model.setProperty("/rules", this.rules);
    this.model.setProperty("/dataTypes", this.dataTypes);

    this._formFragments = {};

    const eventBus = sap.ui.getCore().getEventBus();
    this._datasetService = new DatasetService(this.model, eventBus);
    this._mappingTableService = new MappingTableService(this.model, eventBus);
    this._controller = controller;
    const dialogFactory = new JoinConditionDialogFactory(this.controller, sap.ui.core.Fragment.load);
    this._addJoinConditionDialog = dialogFactory.getDialog();
  }

  get controller() {
    return this._controller;
  }

  get model() {
    return this._model;
  }

  get formFragments() {
    return this._formFragments;
  }

  get datasetService() {
    return this._datasetService;
  }

  get mappingTableService() {
    return this._mappingTableService;
  }

  get addJoinConditionDialog() {
    return this._addJoinConditionDialog;
  }

  get rules() {
    return [
      {
        _t: "CastingConformanceRule",
        schemaFieldSelectorSupportedRule: true
      },
      {
        _t: "ConcatenationConformanceRule",
        schemaFieldSelectorSupportedRule: false
      },
      {
        _t: "DropConformanceRule",
        schemaFieldSelectorSupportedRule: true
      },
      {
        _t: "LiteralConformanceRule",
        schemaFieldSelectorSupportedRule: false
      },
      {
        _t: "MappingConformanceRule",
        schemaFieldSelectorSupportedRule: true
      },
      {
        _t: "NegationConformanceRule",
        schemaFieldSelectorSupportedRule: true
      },
      {
        _t: "SingleColumnConformanceRule",
        schemaFieldSelectorSupportedRule: true
      },
      {
        _t: "SparkSessionConfConformanceRule",
        schemaFieldSelectorSupportedRule: false
      },
      {
        _t: "UppercaseConformanceRule",
        schemaFieldSelectorSupportedRule: true
      }
    ]
  }

  get dataTypes() {
    return [
      {type: "boolean"},
      {type: "byte"},
      {type: "short"},
      {type: "integer"},
      {type: "long"},
      {type: "float"},
      {type: "double"},
      {type: "decimal(38,18)"},
      // {type: "char"}, // TODO: First resolve https://github.com/AbsaOSS/enceladus/issues/425
      {type: "string"},
      {type: "date"},
      {type: "timestamp"}
    ]
  }

  onBeforeOpen() {
    this._dialog = this.controller.byId("upsertConformanceRuleDialog");
    this._datasetSchemaFieldSelector = new ConformanceRuleSchemaFieldSelector(this, this._dialog);
    this._targetAttributeSelector = new TargetAttributeFieldSelector(this, this._dialog);
    this._ruleForm = this.controller.byId("ruleForm");
    this.mappingTableService.getList(this._dialog);
    if (this.model.getProperty("/newRule/isEdit")) {
      this.showFormFragment(this.model.getProperty("/newRule/_t"));
    } else {
      this.model.setProperty("/newRule/_t", this.rules[0]._t);
      this.showFormFragment(this.rules[0]._t);
    }
    this._dialog.setEscapeHandler(() => this.onClosePress());
  }

  onAfterOpen() {
    let schemaFieldSelectorSupportedRules =
      this.rules.filter(rule => rule.schemaFieldSelectorSupportedRule).map(rule => rule._t);
    let newRule = this.model.getProperty("/newRule");
    if (newRule.isEdit && schemaFieldSelectorSupportedRules.includes(newRule._t))
      this.preselectSchemaFieldSelector(newRule._t);
  }

  onClosePress() {
    this.resetRuleForm();
    this._dialog.close();
  }

  onRuleSubmit() {
    let currentDataset = this.model.getProperty("/currentDataset");
    let newRule = $.extend(true, {}, this.model.getProperty("/newRule"));
    this.beforeSubmitChanges(newRule);
    if (this.model.getProperty("/newRule/isEdit")) {
      this.updateRule(currentDataset, newRule);
    } else {
      this.addRule(currentDataset, newRule);
    }
    this.onClosePress();
  }

  onAddInputColumn() {
    let currentRule = this.model.getProperty("/newRule");
    let inputColumnSize = currentRule.inputColumns.length;
    let pathToNewInputColumn = "/newRule/inputColumns/" + inputColumnSize;
    this.model.setProperty(pathToNewInputColumn, "");
  }

  onAddJoinCondition() {
    this.addJoinConditionDialog.onAddPress();
  }

  onMappingTableSelect(oEv) {
    this.resetTargetAttribute();
    this.resetJoinConditions();

    let mappingTableName = oEv.getParameter("selectedItem").getKey();
    this.selectMappingTable(mappingTableName);
  }

  selectMappingTable(sMappingTableId) {
    this.mappingTableService
      .getAllVersions(sMappingTableId, sap.ui.getCore().byId("mappingTableVersionSelect"), this.model, "/newRule/mappingTableVersion")
      .then(() => this.selectMappingTableVersion());
  }

  onMTVersionSelect(oEv) {
    this.resetTargetAttribute();
    this.resetJoinConditions();

    this.selectMappingTableVersion();
  }

  selectMappingTableVersion() {
    let mappingTableId = this.model.getProperty("/newRule/mappingTable");
    let mappingTableVersion = this.model.getProperty("/newRule/mappingTableVersion");

    new MappingTableRestDAO().getByNameAndVersion(mappingTableId, mappingTableVersion).then(mappingTable => {
      const schemaRestDAO = new SchemaRestDAO();
      schemaRestDAO.getByNameAndVersion(mappingTable.schemaName, mappingTable.schemaVersion).then(mappingTableSchema => {
        this.addJoinConditionDialog.setMappingTableSchema(mappingTableSchema);
        if (this.model.getProperty("/newRule/_t") === "MappingConformanceRule") {
          const targetAttributeSelector = sap.ui.getCore().byId("MappingConformanceRule--schemaFieldSelector");
          targetAttributeSelector.setModel(new sap.ui.model.json.JSONModel(mappingTableSchema), "schema")
        }
      });
      const datasetSchema = this._dialog.getModel("schema").oData;
      this.addJoinConditionDialog.setDatasetSchema(datasetSchema);
    });
  }

  resetTargetAttribute() {
    this._targetAttributeSelector.reset(this._ruleForm);
    this.model.setProperty("/newRule/targetAttribute", null);
  }

  resetJoinConditions() {
    this._addJoinConditionDialog.reset();
    this.model.setProperty("/newRule/newJoinConditions", []);
  }

  onRuleSelect() {
    this.resetRuleForm();
    this.showFormFragment(this.model.getProperty("/newRule/_t"));
  }

  onDeleteInputColumn(oEv) {
    let sBindPath = oEv.getParameter("listItem").getBindingContext().getPath();
    let toks = sBindPath.split("/");
    let inputColumnIndex = parseInt(toks[toks.length - 1]);
    let oldInputColumns = this.model.getProperty("/newRule/inputColumns");

    let newInputColumns = oldInputColumns.filter((_, index) => index !== inputColumnIndex);
    this.model.setProperty("/newRule/inputColumns", newInputColumns);
  }

  onDeleteJoinCondition(oEv) {
    let sBindPath = oEv.getParameter("listItem").getBindingContext().getPath();
    let toks = sBindPath.split("/");
    let inputColumnIndex = parseInt(toks[toks.length - 1]);
    let oldInputColumns = this.model.getProperty("/newRule/newJoinConditions");

    let newInputColumns = oldInputColumns.filter((_, index) => index !== inputColumnIndex);
    this.model.setProperty("/newRule/newJoinConditions", newInputColumns);
  }

  onJoinConditionSelect(oEv) {
    const item = oEv.getSource();
    const datasetField = item.data("datasetField");
    const mappingTableField = item.data("mappingTableField");
    const index = item.getParent().indexOfItem(item);

    this.addJoinConditionDialog.onEditPress(index, datasetField, mappingTableField);
  }

  onSchemaFieldSelect(oEv) {
    let ruleType = this._model.getProperty("/newRule/_t");

    switch(ruleType) {
      case "MappingConformanceRule":
        this._targetAttributeSelector.onSchemaFieldSelect(oEv, "/newRule/targetAttribute");
        break;
      case "DropConformanceRule":
        this._datasetSchemaFieldSelector.onSchemaFieldSelect(oEv, "/newRule/outputColumn");
        break;
      default:
        this._datasetSchemaFieldSelector.onSchemaFieldSelect(oEv, "/newRule/inputColumn");
    }
  }

  preselectSchemaFieldSelector(ruleType) {
    switch(ruleType) {
      case "MappingConformanceRule":
        this._targetAttributeSelector.preselectSchemaFieldSelector(this.model.getProperty("/newRule/targetAttribute"));
        break;
      case "DropConformanceRule":
        this._datasetSchemaFieldSelector.preselectSchemaFieldSelector(this.model.getProperty("/newRule/outputColumn"), ruleType);
        break;
      default:
        this._datasetSchemaFieldSelector.preselectSchemaFieldSelector(this.model.getProperty("/newRule/inputColumn"), ruleType);
    }
  }

  beforeShowFragmentChanges() {
    let currentRule = this.model.getProperty("/newRule");
    let newRule = currentRule;

    if (currentRule._t === "ConcatenationConformanceRule" && !currentRule.isEdit) {
      newRule.inputColumns = ["", ""];
    }

    if (currentRule._t === "MappingConformanceRule") {
      if (!currentRule.isEdit) {
        newRule.newJoinConditions = [];
        newRule.mappingTable = this._dialog.getModel("mappingTables").oData[0]._id;
        newRule.mappingTableVersion = this._dialog.getModel("mappingTables").oData[0].latestVersion;
      } else {
        let oAttributeMappings = newRule.attributeMappings;
        let aNewJoinConditions = [];
        for (let key in oAttributeMappings) {
          aNewJoinConditions.push({
            mappingTableField: key,
            datasetField: oAttributeMappings[key]
          });
        }
        newRule.newJoinConditions = aNewJoinConditions;
      }
      this.selectMappingTable(newRule.mappingTable)
    }

    if (!currentRule.isEdit) {
      newRule.order = this.model.getProperty("/currentDataset").conformance.length;
    }

    this.model.setProperty("/newRule", newRule);
  }

  beforeSubmitChanges(newRule) {
    if (newRule._t === "MappingConformanceRule") {
      newRule.attributeMappings = {};
      newRule.newJoinConditions.map(function (joinCondition) {
        newRule.attributeMappings[joinCondition.mappingTableField] = joinCondition.datasetField
      });
      delete newRule.joinConditions;
    }
  }

  addRule(currentDataset, newRule) {
    this.updateRule(currentDataset, newRule)
  }

  updateRule(currentDataset, newRule) {
    currentDataset.conformance[newRule.order] = newRule;
    sap.ui.getCore().getEventBus().publish("conformance", "updated", currentDataset.conformance);
    this.datasetService.update(currentDataset);
  }

  showFormFragment(sFragmentName) {
    let aFragment = this.getFormFragment(sFragmentName);
    aFragment.forEach(oElement =>
      this._ruleForm.addContent(oElement)
    );
    this.beforeShowFragmentChanges();
  }

  getFormFragment(sFragmentName) {
    let oFormFragment = this.formFragments[sFragmentName];
    if (oFormFragment) {
      return oFormFragment;
    }

    oFormFragment = sap.ui.xmlfragment(sFragmentName, "components.dataset.conformanceRule." + sFragmentName + ".add", this);
    this.formFragments[sFragmentName] = oFormFragment;
    return this.formFragments[sFragmentName];
  }

  resetRuleForm() {
    // workaround for "Cannot read property 'setSelectedIndex' of undefined" error
    this._datasetSchemaFieldSelector.reset(this._ruleForm);
    this._ruleForm.removeAllContent();
  }

}
