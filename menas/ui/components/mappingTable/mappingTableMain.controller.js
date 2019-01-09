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

jQuery.sap.require("sap.m.MessageToast")
jQuery.sap.require("sap.m.MessageItem");
jQuery.sap.require("sap.m.MessageBox");
jQuery.sap.require("sap.m.MessagePopover");
sap.ui.controller("components.mappingTable.mappingTableMain", {

  /**
   * Called when a controller is instantiated and its View controls (if available) are already created. Can be used to
   * modify the View before it is displayed, to bind event handlers and do other one-time initialization.
   *
   * @memberOf components.mappingTable.mappingTableMain
   */
  onInit : function() {
    this._model = sap.ui.getCore().getModel()
    this._router = sap.ui.core.UIComponent.getRouterFor(this)
    this._router.getRoute("mappingTables").attachMatched(function(oEvent) {
      var arguments = oEvent.getParameter("arguments")
      this.routeMatched(arguments);
    }, this);

    this._addDialog = sap.ui.xmlfragment("components.mappingTable.addMappingTable", this);
    sap.ui.getCore().byId("newMappingTableAddButton").attachPress(this.MTAddSubmit, this)
    sap.ui.getCore().byId("newMappingTableCancelButton").attachPress(this.MTAddCancel, this)
    sap.ui.getCore().byId("newMappingTableName").attachChange(this.mappingTableNameChange, this)
    sap.ui.getCore().byId("newMappingTableSchemaNameSelect").attachChange(this.schemaSelect, this)
    this._addDialog.setBusyIndicatorDelay(0)

    this._addDefaultDialog = sap.ui.xmlfragment("components.mappingTable.addDefaultValue", this);
    sap.ui.getCore().byId("newDefaultValueAddButton").attachPress(this.defaultSubmit, this)
    sap.ui.getCore().byId("newDefaultValueCancelButton").attachPress(this.defaultCancel, this)
    sap.ui.getCore().byId("addDefaultValueDialog").attachAfterOpen(this.defaultDialogAfterOpen, this)

    this._addDefaultDialog.setBusyIndicatorDelay(0)
  },

  onAddDefaultPress : function() {
    this._model.setProperty("/newDefaultValue", {
      title : "Add"
    })
    this._addDefaultDialog.open();
  },

  defaultDialogAfterOpen : function(oEv) {
    var scrollCont = sap.ui.getCore().byId("defValFieldSelectScroll");
    var selected = sap.ui.getCore().byId("newDefaultValueFieldSelector").getSelectedItem()

    // this needs to be already rendered for it to work
    setTimeout(function() {
      scrollCont.scrollToElement(selected, 500);
    }, 1000);
  },

  defaultCancel : function() {
    // This is a workaround for a bug in the Tree component of 1.56.x and 1.58.x
    // TODO: verify whether this was fixed in the subsequent versions
    var tree = sap.ui.getCore().byId("newDefaultValueFieldSelector")
    var items = tree.getItems()
    for ( var i in items) {
      items[i].setSelected(false)
    }

    this.resetNewDefaultValueState();
    this._addDefaultDialog.close();
  },

  resetNewDefaultValueState : function(oEv) {
    sap.ui.getCore().byId("newDefaultValueExpr").setValueState(sap.ui.core.ValueState.None)
    sap.ui.getCore().byId("newDefaultValueExpr").setValueStateText("")

    var items = sap.ui.getCore().byId("newDefaultValueFieldSelector").getItems();
    for ( var ind in items) {
      items[ind].setHighlight(sap.ui.core.MessageType.None)
    }
  },

  validateNewDefaultValue : function() {
    this.resetNewDefaultValueState();
    var oDef = this._model.getProperty("/newDefaultValue")
    var isOk = true

    if (!oDef.value || oDef.value === "") {
      sap.ui.getCore().byId("newDefaultValueExpr").setValueState(sap.ui.core.ValueState.Error)
      sap.ui.getCore().byId("newDefaultValueExpr").setValueStateText("Default value cannot be empty")
      isOk = false;
    }

    if (!oDef.columnName || oDef.columnName === "") {
      var items = sap.ui.getCore().byId("newDefaultValueFieldSelector").getItems();
      for ( var ind in items) {
        items[ind].setHighlight(sap.ui.core.MessageType.Error)
      }
      sap.m.MessageToast.show("Please choose the target column for this default value")
      isOk = false;
    }
    return isOk;
  },

  schemaFieldSelect : function(oEv) {
    var bind = oEv.getParameter("listItem").getBindingContext().getPath();
    sap.ui.getCore().getModel().setProperty("/newDefaultValue/columnName", this._buildSchemaPath(bind));
  },

  _buildSchemaPath : function(sBindingPath) {
    var modelPathBase = "/currentMappingTable/schema/fields/"

      var pathToks = sBindingPath.replace(modelPathBase, "").split("/")
      var model = sap.ui.getCore().getModel()

      var helper = function(aToks, sModelPathAcc, aAcc) {
      if (aToks.length === 0)
        return aAcc.join(".")

        var rev = aToks.reverse()
        var sCurrPath = sModelPathAcc + rev.pop() + "/";

      var curr = model.getProperty(sCurrPath)
      aAcc.push(curr.name)

      var newPath = sCurrPath + rev.pop() + "/";

      return helper(rev.reverse(), newPath, aAcc)
    }

    return helper(pathToks, modelPathBase, [])
  },

  defaultSubmit : function() {
    var newDef = this._model.getProperty("/newDefaultValue")
    var bindPath = newDef["bindPath"];

    var currMT = this._model.getProperty("/currentMappingTable")

    if (this.validateNewDefaultValue()) {
      // send and update UI
      if (newDef.isEdit) {
        var defs = currMT["defaultMappingValue"].map(function(el) {
          return {
            columnName : el.columnName,
            value : el.value
          };
        });

        MappingTableService.editDefaultValues(currMT.name, currMT.version, defs);
      } else {
        MappingTableService.addDefault(currMT.name, currMT.version, newDef)
      }
      this.defaultCancel(); // close & clean up
    }
  },

  _schemaFieldSelectorSelectPath : function(sExpandTo) {

    var aTokens = sExpandTo.split(".");
    var oCtl = sap.ui.getCore().byId("newDefaultValueFieldSelector")

    oCtl.collapseAll();

    var sRealPath = "/currentMappingTable/schema/fields"
      // var sNewPath = "/currentMappingTable/schema/children"

      var model = sap.ui.getCore().getModel();
    // model.setProperty(sNewPath, model.getProperty(sRealPath))

    var helper = function(aToks, sModelPathAcc) {
      if (aToks.length === 0) {
        var items = oCtl.getItems()
        for ( var i in items) {
          var p = items[i].getBindingContext().getPath()
          var modelPath = sModelPathAcc.substring(0, sModelPathAcc.length - 9) // substring
          // to get rid of the children suffix
          if (p === modelPath) {
            oCtl.setSelectedItem(items[i]);
            sap.ui.getCore().byId("defValFieldSelectScroll").scrollToElement(items[i])
          }
        }
      } else {
        var curr = model.getProperty(sModelPathAcc);
        for ( var i in curr) {
          if (curr[i]["name"] === aToks[0]) {
            var newPath = sModelPathAcc + "/" + i + "/children"
            var items = oCtl.getItems()
            for ( var x in items) {
              var itemPath = items[x].data("path") + "." + items[x].getTitle()
              var modelPath = curr[i]["path"] + "." + curr[i]["name"]
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

    }

    helper(aTokens, "/currentMappingTable/schema/fields")
  },

  onDefaultValueMenuAction : function(oEv) {
    var sAction = oEv.getParameter("item").data("action")
    var sBindPath = oEv.getParameter("item").getBindingContext().getPath();

    if (sAction === "edit") {
      var old = this._model.getProperty(sBindPath);
      old.title = "Edit";
      old.isEdit = true;
      old.bindPath = sBindPath;
      this._model.setProperty("/newDefaultValue", old);
      this._addDefaultDialog.open();
      this._schemaFieldSelectorSelectPath(old["columnName"])
    } else if (sAction === "delete") {
      sap.m.MessageBox.confirm("Are you sure you want to delete the default value?", {
        actions: [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO],
        onClose: function(oResponse) {
          if(oResponse == sap.m.MessageBox.Action.YES) {
            var toks = sBindPath.split("/")
            var index = toks[toks.length-1]
            var currMT = this._model.getProperty("/currentMappingTable")
            var defs = currMT["defaultMappingValue"].filter((el, ind) => ind !== parseInt(index))
            MappingTableService.editDefaultValues(currMT.name, currMT.version, defs);
          }
        }.bind(this)
      });
    }
  },

  metadataPress : function(oEv) {
    var binding = oEv.getSource().getBindingContext().getPath() + "/metadata";
    var bindingArr = binding + "Arr";
    // hmm bindAggregation doesn't take formatter :-/
    var arrMeta = Formatters.objToKVArray(this._model.getProperty(binding))
    this._model.setProperty(bindingArr, arrMeta)

    var oMessageTemplate = new sap.m.MessageItem({
      title : '{key}',
      subtitle : '{value}',
      type : sap.ui.core.MessageType.None
    });

    var oMessagePopover = new sap.m.MessagePopover({
      items : {
        path : bindingArr,
        template : oMessageTemplate
      }
    }).setModel(this._model);

    oMessagePopover.toggle(oEv.getSource());
  },

  MTAddCancel : function() {
    // This is a workaround for a bug in the Tree component of 1.56.5
    // TODO: verify whether this was fixed in the subsequent versions
    var tree = sap.ui.getCore().byId("addMtHDFSBrowser")
    tree.unselectAll();

    tree.collapseAll();
    this.resetNewMappingValueState();
    this._addDialog.close();
  },

  MTAddSubmit : function() {
    var newMT = this._model.getProperty("/newMappingTable")
    // we may wanna wait for a call to determine whether this is unique
    if (!newMT.isEdit && newMT.name && typeof (newMT.nameUnique) === "undefined") {
      // need to wait for the service call
      setTimeout(this.MTAddSubmit.bind(this), 500);
      return;
    }
    if (this.validateNewMappingTable()) {
      // send and update UI
      if (newMT.isEdit) {
        MappingTableService.editMappingTable(newMT.name, newMT.version, newMT.description, newMT.hdfsPath, newMT.schemaName, newMT.schemaVersion)
      } else {
        MappingTableService.createMappingTable(newMT.name, newMT.description, newMT.hdfsPath, newMT.schemaName, newMT.schemaVersion)
      }
      this.MTAddCancel(); // close & clean up
    }
  },

  resetNewMappingValueState : function() {
    sap.ui.getCore().byId("newMappingTableName").setValueState(sap.ui.core.ValueState.None)
    sap.ui.getCore().byId("newMappingTableName").setValueStateText("")

    sap.ui.getCore().byId("newMappingTableSchemaNameSelect").setValueState(sap.ui.core.ValueState.None)
    sap.ui.getCore().byId("newMappingTableSchemaNameSelect").setValueStateText("")

    sap.ui.getCore().byId("newMappingTableSchemaVersionSelect").setValueState(sap.ui.core.ValueState.None)
    sap.ui.getCore().byId("newMappingTableSchemaVersionSelect").setValueStateText("")

    sap.ui.getCore().byId("addMtHDFSBrowser").setValueState(sap.ui.core.ValueState.None)
  },

  validateNewMappingTable : function() {
    this.resetNewMappingValueState();
    var oMT = this._model.getProperty("/newMappingTable")
    var isOk = true

    if (!oMT.name || oMT.name === "") {
      sap.ui.getCore().byId("newMappingTableName").setValueState(sap.ui.core.ValueState.Error)
      sap.ui.getCore().byId("newMappingTableName").setValueStateText("Mapping Table name cannot be empty")
      isOk = false;
    } else if (!oMT.isEdit && !oMT.nameUnique) {
      sap.ui.getCore().byId("newMappingTableName").setValueState(sap.ui.core.ValueState.Error)
      sap.ui.getCore().byId("newMappingTableName").setValueStateText("Mapping Table name '" + oMT.name + "' already exists. Choose a different name.")
      isOk = false;
    }
    if (GenericService.isValidEntityName(oMT.name)) {
      sap.ui.getCore().byId("newMappingTableName").setValueState(sap.ui.core.ValueState.Error)
      sap.ui.getCore().byId("newMappingTableName").setValueStateText("Mapping Table name '" + oMT.name + "' should not have spaces. Please remove spaces and retry")
      isOk = false;
    }
    if (!oMT.schemaName || oMT.schemaName === "") {
      sap.ui.getCore().byId("newMappingTableSchemaNameSelect").setValueState(sap.ui.core.ValueState.Error)
      sap.ui.getCore().byId("newMappingTableSchemaNameSelect").setValueStateText("Please choose the schema of the mapping table")
      isOk = false;
    }
    if (oMT.schemaVersion === undefined || oMT.schemaVersion === "") {
      sap.ui.getCore().byId("newMappingTableSchemaVersionSelect").setValueState(sap.ui.core.ValueState.Error)
      sap.ui.getCore().byId("newMappingTableSchemaVersionSelect").setValueStateText("Please choose the version of the schema for the mapping table")
      isOk = false;
    }
    if (!oMT.hdfsPath || oMT.hdfsPath === "/") {
      sap.ui.getCore().byId("addMtHDFSBrowser").setValueState(sap.ui.core.ValueState.Error)

      sap.m.MessageToast.show("Please choose the HDFS path of the mapping table")
      isOk = false;
    }
    return isOk;
  },

  onAfterRendering : function() {
    // get schemas after rendering. This will be used for add/edit
    // functionality
    var schemas = this._model.getProperty("/schemas")
    if (!schemas || schemas.length === 0) {
      SchemaService.getSchemaList(false, true);
    }
  },

  onEditPress : function() {
    var current = this._model.getProperty("/currentMappingTable");

    current.isEdit = true;
    current.title = "Edit";

    this._model.setProperty("/newMappingTable", current);

    SchemaService.getAllSchemaVersions(current.schemaName, sap.ui.getCore().byId("newMappingTableSchemaVersionSelect"))

    this._addDialog.open();
  },

  _loadAllVersionsOfFirstSchema : function() {
    this._model.setProperty("/newMappingTable", {
      isEdit : false,
      title : "Add"
    });

    var schemas = this._model.getProperty("/schemas")

    if (schemas.length > 0) {
      this._model.setProperty("/newSchema", {
        schemaName : schemas[0]._id
      });

      var sSchema = this._model.getProperty("/schemas/0/_id")
      SchemaService.getAllSchemaVersions(sSchema, sap.ui.getCore().byId("newMappingTableSchemaVersionSelect"))
    }
  },

  onAddPress : function() {
    this._loadAllVersionsOfFirstSchema();

    this._addDialog.open();
  },

  schemaSelect : function(oEv) {
    var sSchemaId = oEv.getParameter("selectedItem").getKey()
    SchemaService.getAllSchemaVersions(sSchemaId, sap.ui.getCore().byId("newMappingTableSchemaVersionSelect"))
  },

  mappingTableSelected : function(oEv) {
    var selected = oEv.getParameter("listItem").data("id")
    this._router.navTo("mappingTables", {
      id : selected
    });
  },

  mappingTableNameChange : function() {
    MappingTableService.isUniqueMappingName(this._model.getProperty("/newMappingTable/name"))
  },

  onRemovePress : function(oEv) {
    var current = this._model.getProperty("/currentMappingTable")

    sap.m.MessageBox.show("This action will remove ALL versions of the mapping table definition. \nAre you sure?.", {
      icon : sap.m.MessageBox.Icon.WARNING,
      title : "Are you sure?",
      actions : [ sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO ],
      onClose : function(oAction) {
        if (oAction == "YES") {
          MappingTableService.disableMappingTable(current.name)
        }
      }
    });
  },

  routeMatched : function(oParams) {
    if (Prop.get(oParams, "id") == undefined) {
      MappingTableService.getMappingTableList(true, true);
    } else if (Prop.get(oParams, "version") == undefined) {
      MappingTableService.getMappingTableList();
      MappingTableService.getLatestMappingTableVersion(oParams.id, true)
    } else {
      MappingTableService.getMappingTableList();
      MappingTableService.getMappingTableVersion(oParams.id, oParams.version, true)
    }
  },

  toSchema : function(oEv) {
    var src = oEv.getSource();
    sap.ui.core.UIComponent.getRouterFor(this).navTo("schemas", {
      id : src.data("name"),
      version : src.data("version")
    })
  },

  fetchSchema : function(oEv) {
    var mappingTable = sap.ui.getCore().getModel().getProperty("/currentMappingTable")
    if (typeof (mappingTable.schema) === "undefined") {
      SchemaService.getSchemaVersion(mappingTable.schemaName, mappingTable.schemaVersion, "/currentMappingTable/schema")
    }
  },

  tabSelect : function(oEv) {
    if (oEv.getParameter("selectedKey") === "schema")
      this.fetchSchema();
  }

  /**
   * Similar to onAfterRendering, but this hook is invoked before the controller's View is re-rendered (NOT before the
   * first rendering! onInit() is used for that one!).
   *
   * @memberOf components.mappingTable.mappingTableMain
   */
//onBeforeRendering: function() {

//},
  /**
   * Called when the View has been rendered (so its HTML is part of the document). Post-rendering manipulations of the
   * HTML could be done here. This hook is the same one that SAPUI5 controls get after being rendered.
   *
   * @memberOf components.mappingTable.mappingTableMain
   */

  /**
   * Called when the Controller is destroyed. Use this one to free resources and finalize activities.
   *
   * @memberOf components.mappingTable.mappingTableMain
   */
//onExit: function() {

//}
});
