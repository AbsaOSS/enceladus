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

  return Controller.extend("components.mappingTable.mappingTableDetail", {

    /**
     * Called when a controller is instantiated and its View controls (if available) are already created. Can be used to
     * modify the View before it is displayed, to bind event handlers and do other one-time initialization.
     *
     * @memberOf components.mappingTable.mappingTableDetail
     */
    onInit: function () {
      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("mappingTables").attachMatched(function (oEvent) {
        let args = oEvent.getParameter("arguments");
        this.routeMatched(args);
      }, this);

      this._editFragment = new AddMappingTableFragment(this, Fragment.load).getEdit();

      this._addDefaultDialog = sap.ui.xmlfragment("components.mappingTable.addDefaultValue", this);
      sap.ui.getCore().byId("newDefaultValueAddButton").attachPress(this.defaultSubmit, this);
      sap.ui.getCore().byId("newDefaultValueCancelButton").attachPress(this.defaultCancel, this);
      sap.ui.getCore().byId("addDefaultValueDialog").attachAfterOpen(this.defaultDialogAfterOpen, this);

      this._addDefaultDialog.setBusyIndicatorDelay(0);

      const eventBus = sap.ui.getCore().getEventBus();
      this._mappingTableService = new MappingTableService(this._model, eventBus)
      this._schemaService = new SchemaService(this._model, eventBus)
      this._schemaTable = new SchemaTable(this)
    },

    auditVersionPress: function (oEv) {
      let oSrc = oEv.getParameter("listItem");
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
      let scrollCont = sap.ui.getCore().byId("defValFieldSelectScroll");
      let selected = sap.ui.getCore().byId("schemaFieldSelector").getSelectedItem();

      // this needs to be already rendered for it to work
      setTimeout(function () {
        scrollCont.scrollToElement(selected, 500);
      }, 1000);
    },

    defaultCancel: function () {
      // This is a workaround for a bug in the Tree component of 1.56.x and 1.58.x
      // TODO: verify whether this was fixed in the subsequent versions
      let tree = sap.ui.getCore().byId("schemaFieldSelector");
      let items = tree.getItems();
      for (let i in items) {
        items[i].setSelected(false)
      }

      this.resetNewDefaultValueState();
      this._addDefaultDialog.close();
    },

    resetNewDefaultValueState: function (oEv) {
      sap.ui.getCore().byId("newDefaultValueExpr").setValueState(sap.ui.core.ValueState.None)
      sap.ui.getCore().byId("newDefaultValueExpr").setValueStateText("");

      let items = sap.ui.getCore().byId("schemaFieldSelector").getItems();
      for (let ind in items) {
        items[ind].setHighlight(sap.ui.core.MessageType.None)
      }
    },

    validateNewDefaultValue: function () {
      this.resetNewDefaultValueState();
      let oDef = this._model.getProperty("/newDefaultValue")
      let isOk = true

      if (!oDef.value || oDef.value === "") {
        sap.ui.getCore().byId("newDefaultValueExpr").setValueState(sap.ui.core.ValueState.Error)
        sap.ui.getCore().byId("newDefaultValueExpr").setValueStateText("Default value cannot be empty")
        isOk = false;
      }

      if (!oDef.columnName || oDef.columnName === "") {
        let items = sap.ui.getCore().byId("schemaFieldSelector").getItems();
        for (let ind in items) {
          items[ind].setHighlight(sap.ui.core.MessageType.Error)
        }
        MessageToast.show("Please choose the target column for this default value")
        isOk = false;
      }
      return isOk;
    },

    onSchemaFieldSelect: function (oEv) {
      let bind = oEv.getParameter("listItem").getBindingContext("schema").getPath();
      let modelPathBase = "/fields/";
      let model = this._addDefaultDialog.getModel("schema");
      this._schemaService.fieldSelect(bind, modelPathBase, model, "/newDefaultValue/columnName");
    },

    defaultSubmit: function () {
      let newDef = this._model.getProperty("/newDefaultValue")

      let currentMT = this._model.getProperty("/currentMappingTable")

      if (this.validateNewDefaultValue()) {
        // send and update UI
        if (newDef.isEdit) {
          let bindPath = newDef.bindPath;
          this._model.setProperty(bindPath, {
            columnName: newDef.columnName,
            value: newDef.value
          });

          this._mappingTableService.editDefaultValues(currentMT.name, currentMT.version, currentMT.defaultMappingValue);
        } else {
          this._mappingTableService.addDefaultValue(currentMT.name, currentMT.version, newDef)
        }
        this.defaultCancel(); // close & clean up
      }
    },

    _schemaFieldSelectorSelectPath: function (sExpandTo) {

      let aTokens = sExpandTo.split(".");
      let oCtl = sap.ui.getCore().byId("schemaFieldSelector");

      oCtl.collapseAll();

      let model = this._addDefaultDialog.getModel("schema");

      let helper = function (aToks, sModelPathAcc) {
        if (aToks.length === 0) {
          let items = oCtl.getItems();
          for (let i in items) {
            let p = items[i].getBindingContext("schema").getPath();
            // substring to get rid of the children suffix
            let modelPath = sModelPathAcc.substring(0, sModelPathAcc.length - 9);
            if (p === modelPath) {
              console.log(items[i]);
              oCtl.setSelectedItem(items[i]);
              sap.ui.getCore().byId("defValFieldSelectScroll").scrollToElement(items[i])
            }
          }
        } else {
          let curr = model.getProperty(sModelPathAcc);
          for (let i in curr) {
            if (curr[i]["name"] === aToks[0]) {
              let newPath = sModelPathAcc + "/" + i + "/children";
              let items = oCtl.getItems();
              for (let x in items) {
                let itemPath = items[x].data("path") + "." + items[x].getTitle();
                let modelPath = curr[i]["path"] + "." + curr[i]["name"];
                if (itemPath === modelPath) {
                  oCtl.expand(parseInt(x));
                  break;
                }
              }
              helper(aToks.slice(1), newPath);
              return;
            }
          }
        }

      };

      helper(aTokens, "/fields")
    },

    onDefaultValueMenuAction: function (oEv) {
      let sAction = oEv.getParameter("item").data("action")
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

    onEditPress: function () {
      this._editFragment.onPress();
    },

    onMTSubmit: function () {
      this._editFragment.submit();
    },

    onMTCancel: function () {
      this._editFragment.cancel();
    },

    onNameChange: function () {
      this._editFragment.onNameChange();
    },

    onSchemaSelect: function (oEv) {
      this._editFragment.onSchemaSelect(oEv);
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
      if (typeof (currentMT.schema) === "undefined") {
        this._schemaService.getByNameAndVersion(currentMT.schemaName, currentMT.schemaVersion, "/currentMappingTable/schema")
      }
    },

    usedInNavTo: function (oEv) {
      let source = oEv.getSource();
      sap.ui.core.UIComponent.getRouterFor(this).navTo(source.data("collection"), {
        id: source.data("name"),
        version: source.data("version")
      })
    },

    load: function() {
      let currentMT = this._model.getProperty("/currentMappingTable");
      this.byId("info").setModel(new sap.ui.model.json.JSONModel(currentMT), "mappingTable")
      this.fetchSchema();
      const auditTable = this.byId("auditTrailTable");
      this._mappingTableService.getAuditTrail(currentMT.name, auditTable);
    },

    tabSelect: function (oEv) {
      if (oEv.getParameter("selectedKey") === "schemaFields") {
        this._schemaTable.model = this._model.getProperty("/currentMappingTable/schema")
      }
    }

  });
});
