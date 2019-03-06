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
  "sap/ui/core/mvc/Controller"
], function (Controller) {
  return Controller.extend("components.dataset.conformanceRule.upsert", {

    rules: [
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
        schemaFieldSelectorSupportedRule: false
      },
      {
        _t: "LiteralConformanceRule",
        schemaFieldSelectorSupportedRule: false
      },
      {
        _t: "MappingConformanceRule",
        schemaFieldSelectorSupportedRule: false
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

    constructor: function () {
      this._model = sap.ui.getCore().getModel();
      this._model.setProperty("/rules", this.rules);
      this._model.setProperty("/dataTypes", this.dataTypes);
      this._formFragments = {};
    },

    onBeforeOpen: function () {
      this._dialog = sap.ui.getCore().byId("upsertConformanceRuleDialog");
      this._ruleForm = sap.ui.getCore().byId("ruleForm");
      MappingTableService.getMappingTableList(true, true);
      if(this._model.getProperty("/newRule/isEdit")) {
        this.showFormFragment(this._model.getProperty("/newRule/_t"));
      } else {
        this._model.setProperty("/newRule/_t", this.rules[0]._t);
        this.showFormFragment(this.rules[0]._t);
      }
      this._dialog.setEscapeHandler(() => this.onClosePress());
    },

    onAfterOpen: function () {
      let schemaFieldSelectorSupportedRules =
        this.rules.filter(rule => rule.schemaFieldSelectorSupportedRule).map(rule => rule._t);
      let newRule = this._model.getProperty("/newRule");
      if(newRule.isEdit && schemaFieldSelectorSupportedRules.includes(newRule._t))
        this.preselectSchemaFieldSelector(newRule._t);
    },

    onClosePress: function () {
      this.resetRuleForm();
      this._dialog.close();
    },

    onRuleSubmit: function () {
      let currentDataset = this._model.getProperty("/currentDataset");
      let newRule = JSON.parse(JSON.stringify(this._model.getProperty("/newRule")));
      this.beforeSubmitChanges(newRule);
      if(this._model.getProperty("/newRule/isEdit")) {
        this.updateRule(currentDataset, newRule);
      } else {
        this.addRule(currentDataset, newRule);
      }
      this.onClosePress();
    },

    onAddInputColumn: function () {
      let currentRule = this._model.getProperty("/newRule");
      let inputColumnSize = currentRule.inputColumns.length;
      let pathToNewInputColumn = "/newRule/inputColumns/" + inputColumnSize;
      this._model.setProperty(pathToNewInputColumn, "");
    },

    onAddJoinCondition: function () {
      let currentRule = this._model.getProperty("/newRule");
      let joinConditionsSize = currentRule.newJoinConditions.length;
      let pathToNewJoinCondition = "/newRule/newJoinConditions/" + joinConditionsSize;
      this._model.setProperty(pathToNewJoinCondition, {mappingTableField: "", datasetField: ""});
    },

    onMappingTableSelect: function (oEv) {
      let sMappingTableId = oEv.getParameter("selectedItem").getKey();
      MappingTableService.getAllMappingTableVersions(sMappingTableId, sap.ui.getCore().byId("schemaVersionSelect"));
    },

    onRuleSelect: function () {
      this.resetRuleForm();
      this.showFormFragment(this._model.getProperty("/newRule/_t"));
    },

    onDeleteInputColumn: function (oEv) {
      let sBindPath = oEv.getParameter("listItem").getBindingContext().getPath();
      let toks = sBindPath.split("/");
      let inputColumnIndex = parseInt(toks[toks.length - 1]);
      let oldInputColumns = this._model.getProperty("/newRule/inputColumns");

      let newInputColumns = oldInputColumns.filter((_, index) => index !== inputColumnIndex);
      this._model.setProperty("/newRule/inputColumns", newInputColumns);
    },

    onDeleteJoinCondition: function (oEv) {
      let sBindPath = oEv.getParameter("listItem").getBindingContext().getPath();
      let toks = sBindPath.split("/");
      let inputColumnIndex = parseInt(toks[toks.length - 1]);
      let oldInputColumns = this._model.getProperty("/newRule/newJoinConditions");

      let newInputColumns = oldInputColumns.filter((_, index) => index !== inputColumnIndex);
      this._model.setProperty("/newRule/newJoinConditions", newInputColumns);
    },

    onSchemaFieldSelect: function (oEv) {
      let bind = oEv.getParameter("listItem").getBindingContext().getPath();
      let modelPathBase = "/currentDataset/schema/fields/";
      let model = sap.ui.getCore().getModel();
      SchemaService.fieldSelect(bind, modelPathBase, model, "/newRule/inputColumn");
    },

    beforeShowFragmentChanges: function () {
      let currentRule = this._model.getProperty("/newRule");
      let newRule = currentRule;

      if (currentRule._t === "ConcatenationConformanceRule" && !currentRule.isEdit) {
        newRule.inputColumns = ["", ""];
      }

      if (currentRule._t === "MappingConformanceRule") {
        if(!currentRule.isEdit){
          newRule.newJoinConditions = [{mappingTableField: "", datasetField: ""}];
          newRule.mappingTable = this._model.getProperty("/mappingTables")[0]._id;
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
        MappingTableService.getAllMappingTableVersions(newRule.mappingTable, sap.ui.getCore().byId("schemaVersionSelect"));
      }

      if(!currentRule.isEdit) {
        newRule.order = this._model.getProperty("/currentDataset").conformance.length;
      }

      this._model.setProperty("/newRule", newRule);
    },

    beforeSubmitChanges: function (newRule) {
      if (newRule._t === "MappingConformanceRule") {
        newRule.attributeMappings = {};
        newRule.newJoinConditions.map(function(joinCondition) {
          newRule.attributeMappings[joinCondition.mappingTableField] = joinCondition.datasetField
        });
        delete newRule.joinConditions;
      }
    },

    addRule: function (currentDataset, newRule) {
      this.updateRule(currentDataset, newRule)
    },

    updateRule: function (currentDataset, newRule) {
      currentDataset.conformance[newRule.order] = newRule;
      DatasetService.editDataset(currentDataset);
    },

    showFormFragment: function (sFragmentName) {
      let aFragment = this.getFormFragment(sFragmentName);
      aFragment.forEach(oElement =>
        this._ruleForm.addContent(oElement)
      );
      this.beforeShowFragmentChanges();
    },

    getFormFragment: function (sFragmentName) {
      let oFormFragment = this._formFragments[sFragmentName];
      if (oFormFragment) {
        return oFormFragment;
      }

      oFormFragment = sap.ui.xmlfragment(sFragmentName, "components.dataset.conformanceRule." + sFragmentName + ".add", this);
      this._formFragments[sFragmentName] = oFormFragment;
      return this._formFragments[sFragmentName];
    },

    resetRuleForm: function () {
      // workaround for "Cannot read property 'setSelectedIndex' of undefined" error
      const content = this._ruleForm.getContent();
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
      this._ruleForm.removeAllContent();
    },

    preselectSchemaFieldSelector: function(ruleType) {
      let sExpandTo= this._model.getProperty("/newRule/inputColumn");
      let aTokens = sExpandTo.split(".");
      let oCtl = sap.ui.getCore().byId(ruleType + "--schemaFieldSelector");
      let oScr = sap.ui.getCore().byId(ruleType + "--defValFieldSelectScroll");
      oCtl.collapseAll();

      let _preselectRadioButton = function(aToks, oldSize, lastIndex) {
        let newItems = oCtl.getItems();
        let newSize = newItems.length - oldSize;
        let uniqueItems = newItems.slice(lastIndex+1, lastIndex+newSize+1);
        let itemToProcess = uniqueItems.find(item => item.getProperty("title") === aToks[0]);

        if(aToks.length === 1){
          itemToProcess.setSelected(true);
          let delegate = {
            onAfterRendering: function() {
              oScr.scrollToElement(itemToProcess);
              oCtl.removeEventDelegate(delegate);
            }.bind(this)
          };
          oCtl.addEventDelegate(delegate);
        } else {
          let index = newItems.indexOf(itemToProcess);
          let oldSize = newItems.length;
          oCtl.expand(index);
          _preselectRadioButton(aToks.slice(1), oldSize, index);
        }
      };

      _preselectRadioButton(aTokens, 0, -1);
    }

  });
});
