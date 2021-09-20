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
  "sap/m/MessagePopover",
  "components/AuditTrail",
  "components/tables/TableUtils"
], function (Controller, Fragment, MessageToast, MessageItem, MessageBox, MessagePopover, AuditTrail, TableUtils) {
  "use strict";

  return Controller.extend("components.mappingTable.mappingTableDetail", {

    /**
     * Called when a controller is instantiated and its View controls (if available) are already created. Can be used to
     * modify the View before it is displayed, to bind event handlers and do other one-time initialization.
     *
     * @memberOf components.mappingTable.mappingTableDetail
     */
    onInit: function () {
      this._model = sap.ui.getCore().getModel();
      this._oEventBus = sap.ui.getCore().getEventBus();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("mappingTables").attachMatched(function (oEvent) {
        let args = oEvent.getParameter("arguments");
        this.routeMatched(args);
      }, this);

      new MappingTableDialogFactory(this, Fragment.load).getEdit();

      const oView = this.getView();
      Fragment.load({
        id: oView.getId(),
        name: "components.mappingTable.addDefaultValue",
        controller: this
      }).then(function (oDialog) {
        oView.addDependent(oDialog);
        const selectorId = this.createId("schemaFieldSelector");
        const schemaSelector = sap.ui.getCore().byId(selectorId);
        if(schemaSelector) {
          const schemaFieldTableUtils = new TableUtils(schemaSelector, "");
          schemaFieldTableUtils.makeSearchable(["name", "absolutePath"]);
        } else {
          console.log(`No schema field selector matching ${selectorId}, skipping search initialization.`);
        }
      }.bind(this));

      this._addDefaultDialog = this.byId("addDefaultValueDialog");
      this.byId("newDefaultValueAddButton").attachPress(this.defaultSubmit, this);
      this.byId("newDefaultValueCancelButton").attachPress(this.defaultCancel, this);
      this.byId("addDefaultValueDialog").attachAfterOpen(this.defaultDialogAfterOpen, this);

      this._addDefaultDialog.setBusyIndicatorDelay(0);

      this._oEventBus.subscribe("mappingTables", "updated", this.onEntityUpdated, this);

      this._mappingTableService = new MappingTableService(this._model, this._oEventBus);
      this._schemaService = new SchemaService(this._model, this._oEventBus);
      this._schemaTable = new SchemaTable(this, "schemaFragment");
      this._schemaFieldSelector = new SimpleSchemaFieldSelector(this, this._addDefaultDialog)

      const auditTable = this.byId("auditTrailTable");
      const auditUtils = new AuditTrail(auditTable);
      auditUtils.applyTableUtils();
    },

    onEntityUpdated: function (sTopic, sEvent, oData) {
      this._model.setProperty("/currentMappingTable", oData);
      this.load();
    },

    auditVersionPress: function (oEv) {
      let oSrc = oEv.getSource();
      let oRef = oSrc.data("menasRef");
      this._router.navTo("mappingTables", {
        id: oRef.name,
        version: oRef.version
      });
    },

    onAddDefaultPress: function () {
      this._model.setProperty("/newDefaultValue", {
        title: "Add"
      })

      let currentMT = this._model.getProperty("/currentMappingTable");
      this._addDefaultDialog.setModel(new sap.ui.model.json.JSONModel(currentMT.schema), "schema");
      new SchemaRestDAO().getByNameAndVersion(currentMT.schemaName, currentMT.schemaVersion)
        .then(oData => {
          this._addDefaultDialog.getModel("schema").setProperty("/fields", oData.fields);
        });
      this._addDefaultDialog.open();
    },

    defaultDialogAfterOpen: function (oEv) {
      let scrollCont = this.byId("fieldSelectScroll");
      let selected = this.byId("schemaFieldSelector").getSelectedItem();

      // this needs to be already rendered for it to work
      setTimeout(function () {
        scrollCont.scrollToElement(selected, 500);
      }, 1000);
    },

    defaultCancel: function () {
      this._schemaFieldSelector.reset();

      this.resetNewDefaultValueState();
      this._addDefaultDialog.close();
    },

    resetNewDefaultValueState: function (oEv) {
      this.byId("newDefaultValueExpr").setValueState(sap.ui.core.ValueState.None)
      this.byId("newDefaultValueExpr").setValueStateText("");

      let items = this.byId("schemaFieldSelector").getItems();
      for (let ind in items) {
        items[ind].setHighlight(sap.ui.core.MessageType.None)
      }
    },

    validateNewDefaultValue: function () {
      this.resetNewDefaultValueState();
      let oDef = this._model.getProperty("/newDefaultValue")
      let isOk = true

      if (!oDef.value || oDef.value === "") {
        this.byId("newDefaultValueExpr").setValueState(sap.ui.core.ValueState.Error)
        this.byId("newDefaultValueExpr").setValueStateText("Default value cannot be empty")
        isOk = false;
      }

      if (!oDef.columnName || oDef.columnName === "") {
        let items = this.byId("schemaFieldSelector").getItems();
        for (let ind in items) {
          items[ind].setHighlight(sap.ui.core.MessageType.Error)
        }
        MessageToast.show("Please choose the target column for this default value")
        isOk = false;
      }
      return isOk;
    },

    onSchemaFieldSelect: function (oEv) {
      this._schemaFieldSelector.onSchemaFieldSelect(oEv, "/newDefaultValue/columnName");
    },

    _schemaFieldSelectorSelectPath: function (sExpandTo) {
      this._schemaFieldSelector.preselectSchemaFieldSelector(sExpandTo);
    },

    defaultSubmit: function () {
      let newDef = this._model.getProperty("/newDefaultValue");

      let currentMT = this._model.getProperty("/currentMappingTable");

      if (this.validateNewDefaultValue()) {
        // send and update UI
        if (newDef.isEdit) {
          let bindPath = newDef.bindPath;
          this._model.setProperty(bindPath, {
            columnName: newDef.columnName,
            value: newDef.value
          });

          this._mappingTableService.editDefaultValues(currentMT.name, currentMT.version, currentMT.defaultMappingValue)
            .then(() => this.load());
        } else {
          this._mappingTableService.addDefaultValue(currentMT.name, currentMT.version, newDef)
            .then(() => this.load());
        }
        this.defaultCancel(); // close & clean up
      }
    },

    onDefaultValueMenuAction: function (oEv) {
      let sAction = oEv.getParameter("item").data("action");
      let sBindPath = oEv.getParameter("item").getBindingContext().getPath();

      if (sAction === "edit") {
        let old = $.extend(true, {}, this._model.getProperty(sBindPath));
        old.title = "Edit";
        old.isEdit = true;
        old.bindPath = sBindPath;
        this._model.setProperty("/newDefaultValue", old);

        let currentMT = this._model.getProperty("/currentMappingTable");
        this._addDefaultDialog.setModel(new sap.ui.model.json.JSONModel(currentMT.schema), "schema");
        this._addDefaultDialog.open();
        this._schemaFieldSelectorSelectPath(old["columnName"])
      } else if (sAction === "delete") {
        MessageBox.confirm("Are you sure you want to delete the default value?", {
          actions: [MessageBox.Action.YES, MessageBox.Action.NO],
          onClose: function (oResponse) {
            if (oResponse === MessageBox.Action.YES) {
              let toks = sBindPath.split("/");
              let index = toks[toks.length - 1];
              let currentMT = this._model.getProperty("/currentMappingTable");
              let defs = currentMT["defaultMappingValue"].filter((el, ind) => ind !== parseInt(index));
              this._mappingTableService.editDefaultValues(currentMT.name, currentMT.version, defs);
            }
          }.bind(this)
        });
      }
    },

    metadataPress: function (oEv) {
      let binding = oEv.getSource().getBindingContext().getPath() + "/metadata";
      let bindingArr = binding + "Arr";
      // hmm bindAggregation doesn't take formatter :-/
      let arrMeta = Formatters.objToKVArray(this._model.getProperty(binding))
      this._model.setProperty(bindingArr, arrMeta)

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
      let currentMT = this._model.getProperty("/currentMappingTable")

      MessageBox.show("This action will remove all versions of the mapping table definition. \nAre you sure?", {
        icon: MessageBox.Icon.WARNING,
        title: "Are you sure?",
        actions: [MessageBox.Action.YES, MessageBox.Action.NO],
        onClose: (oAction) => {
          if (oAction === "YES") {
            this._mappingTableService.disable(currentMT.name)
          }
        }
      });
    },

    routeMatched: function (oParams) {
      if (Prop.get(oParams, "id") === undefined) {
        this._mappingTableService.getTop().then(() => this.load())
      } else if (Prop.get(oParams, "version") === undefined) {
        this._mappingTableService.getLatestByName(oParams.id).then(() => this.load())
      } else {
        this._mappingTableService.getByNameAndVersion(oParams.id, oParams.version).then(() => this.load())
      }
      this._oEventBus.publish("TableUtils", "clearAllSearch");
      this.byId("mappingTableIconTabBar").setSelectedKey("info");
    },

    toSchema: function (oEv) {
      let src = oEv.getSource();
      sap.ui.core.UIComponent.getRouterFor(this).navTo("schemas", {
        id: src.data("name"),
        version: src.data("version")
      })
    },

    fetchSchema: function (oEv) {
      let currentMT = this._model.getProperty("/currentMappingTable");
      this._schemaService.getByNameAndVersion(currentMT.schemaName, currentMT.schemaVersion, "/currentMappingTable/schema").then((schema) => {
        this._schemaTable.model = schema;
      });
    },

    usedInNavTo: function (oEv) {
      let source = oEv.getSource();
      sap.ui.core.UIComponent.getRouterFor(this).navTo(source.data("collection"), {
        id: source.data("name"),
        version: source.data("version")
      })
    },

    addIconsAndNiceNamesToFilterData: function(filterData){

      // fn to add icon and human readable text
      const applyFn = function(filterNode) {
        switch (filterNode._t) {
          case "AndJoinedFilters":
            filterNode.text = "AND";
            filterNode.icon = "sap-icon://combine";
            break;
          case "OrJoinedFilters":
            filterNode.text = "OR";
            filterNode.icon = "sap-icon://split";
            break;
          case "EqualsFilter":
            filterNode.text = `Value of "${filterNode.columnName}" equals to "${filterNode.value}" (of type ${filterNode.valueType})`;
            filterNode.icon = "sap-icon://filter";
            break;
          case "DiffersFilter":
            filterNode.text = `Value of "${filterNode.columnName}" differs from "${filterNode.value}" (of type ${filterNode.valueType})`;
            filterNode.icon = "sap-icon://clear-filter";
            break;
          case "NotFilter":
            filterNode.text = "NOT";
            filterNode.icon = "sap-icon://SAP-icons-TNT/solution-not-licensed";
            break;
          case "IsNullFilter":
            filterNode.text = `Value of "${filterNode.columnName}" is not null`;
            filterNode.icon = "sap-icon://SAP-icons-TNT/marquee";
            break;
          default:
        }
      };

      return FilterTreeUtils.applyToFilterDataImmutably(filterData, applyFn);
    },

    load: function() {
      let currentMT = this._model.getProperty("/currentMappingTable");

      currentMT.filterJson = JSON.stringify(currentMT.filter);

      let filterDataWithNamesAndIcons = this.addIconsAndNiceNamesToFilterData(currentMT.filter);
      currentMT.filterTree = [filterDataWithNamesAndIcons];

      let tree = this.getView().byId("filterTree");

      this.byId("info").setModel(new sap.ui.model.json.JSONModel(currentMT), "mappingTable");
      if (currentMT) {
        this.fetchSchema();
        const auditTable = this.byId("auditTrailTable");
        this._mappingTableService.getAuditTrail(currentMT.name, auditTable);

        this._mtRestDAO = new MappingTableRestDAO();
        this._mtRestDAO.getLatestVersionByName(currentMT.name)
          .then(version => this._model.setProperty("/editingEnabled", currentMT.version === version));
      }

      tree.expandToLevel(2);
    }
  });
});
