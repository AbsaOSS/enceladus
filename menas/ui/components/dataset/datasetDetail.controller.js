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
  "components/types/NonEmptyArrType",
  "components/validator/Validator",
  "sap/m/MessageToast"
], function (Controller, Fragment, NonEmptyArrType, Validator, MessageToast) {
  "use strict";
  
  return Controller.extend("components.dataset.datasetDetail", {

    /**
     * Called when a controller is instantiated and its View controls (if available) are already created. Can be used to
     * modify the View before it is displayed, to bind event handlers and do other one-time initialization.
     * 
     * @memberOf components.dataset.datasetDetail
     */
    onInit: function () {
      
      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("datasets").attachMatched(function (oEvent) {
        let args = oEvent.getParameter("arguments");
        this.routeMatched(args);
      }, this);

      let cont = new ConformanceRuleDialog(this);
      let view = this.getView();

      this._upsertConformanceRuleDialog = Fragment.load({
        id: view.getId(),
        name:"components.dataset.conformanceRule.upsert",
        controller: cont
      }).then(function (fragment) {
        view.addDependent(fragment);
      });

      this._upsertConformanceRuleDialog = this.byId("upsertConformanceRuleDialog");
      this._editScheduleDialog = sap.ui.xmlfragment("components.dataset.schedule.editSchedule", this);
      sap.ui.getCore().getMessageManager().registerObject(this._editScheduleDialog, true);
      
      new DatasetDialogFactory(this, Fragment.load).getEdit();

      const eventBus = sap.ui.getCore().getEventBus();
      eventBus.subscribe("datasets", "updated", this.onEntityUpdated, this);
      eventBus.subscribe("datasets", "updateFailed", this.onEntityUpdateFailed, this);

      
      this._datasetService = new DatasetService(this._model, eventBus);
      this._mappingTableService = new MappingTableService(this._model, eventBus);
      this._schemaService = new SchemaService(this._model, eventBus)
      this._schemaTable = new SchemaTable(this)
      
      this._validator = new Validator();
      
      this.byId("datasetIconTabBar").attachSelect(oEv => {
        if(oEv.getParameter("selectedKey") === "schedule") {
          OozieService.getCoordinatorStatus();
        }
      });
      
      // Cron time picker
      let cronTemplate = {
        "minute" : this._generateCronTemplateRange(0, 60),
        "hour": this._generateCronTemplateRange(0, 24),
        "dayOfMonth": this._generateCronTemplateRange(1, 32),
        "month": this._generateCronTemplateRange(1, 13),
        "dayOfWeek": this._generateCronTemplateRange(0, 7)
      }
      
      this._model.setProperty("/cronFormTemplate", cronTemplate);
    },
    
    _generateCronTemplateRange: function(iStart, iEnd) {
      return ["*", ...(_.range(iStart, iEnd, 1))].map(n => {
        return {
        "key": n.toString(),
        "name": n.toString()
        }
      });
    },

    onEntityUpdated: function (sTopic, sEvent, oData) {
      this._model.setProperty("/currentDataset", oData);
      OozieService.getCoordinatorStatus();
      this._editScheduleDialog.setBusy(false);
      this._editScheduleDialog.close();
      this.load();
    },

    onEntityUpdateFailed: function() {
      this._editScheduleDialog.setBusy(false);
      this._editScheduleDialog.close();
    },
    
    onAddConformanceRulePress: function () {
      this._model.setProperty("/newRule", {
        title: "Add",
        isEdit: false,
      });

      const rules = this._model.getProperty("/currentDataset/conformance");
      this._setRuleDialogModel(rules);

      this._upsertConformanceRuleDialog.open();
    },

    _setRuleDialogModel: function(rules) {
      const currentDataset = this._model.getProperty("/currentDataset");
      new SchemaRestDAO().getByNameAndVersion(currentDataset.schemaName, currentDataset.schemaVersion)
        .then(schema => {
          schema.fields = SchemaManager.updateTransitiveSchema(schema.fields, rules);
          this._upsertConformanceRuleDialog.setModel(new sap.ui.model.json.JSONModel(schema), "schema");
        });
    },

    onRuleMenuAction: function (oEv) {
      let sAction = oEv.getParameter("item").data("action");
      let sBindPath = oEv.getParameter("item").getBindingContext().getPath();

      if (sAction === "edit") {
        let old = this._model.getProperty(sBindPath);
        this._model.setProperty("/newRule", {
          ...$.extend(true, {}, old),
          title: "Edit",
          isEdit: true,
        });

        const rules = this._model.getProperty("/currentDataset/conformance").slice(0, old.order);
        this._setRuleDialogModel(rules);

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
                this._datasetService.update(newDataset);
              }
            }
          }.bind(this)
        });
      }
    },

    auditVersionPress: function (oEv) {
      let oSrc = oEv.getParameter("listItem");
      let oRef = oSrc.data("menasRef");
      this._router.navTo("datasets", {
        id: oRef.name,
        version: oRef.version
      });
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

    toRun: function (oEv) {
      let src = oEv.getParameter("listItem");
      let datasetName = src.data("datasetName");
      let datasetVersion = src.data("datasetVersion");
      let runId = src.data("runId");

      this._router.navTo("runs", {
        dataset: datasetName,
        version: datasetVersion,
        id: runId
      });
    },

    fetchSchema: function () {
      let currentDataset = this._model.getProperty("/currentDataset");
      if(currentDataset) {
        this._schemaService.getByNameAndVersion(currentDataset.schemaName, currentDataset.schemaVersion, "/currentDataset/schema").then((schema) => {
          this._schemaTable.model = schema;
        });
      }
    },

    fetchRuns: function () {
      let currentDataset = this._model.getProperty("/currentDataset");
      RunService.getDatasetRuns(this.byId("Runs"), currentDataset.name, currentDataset.version);
    },

    mappingTableSelect: function (oEv) {
      let sMappingTableId = oEv.getParameter("selectedItem").getKey();
      this._mappingTableService.getAllVersions(sMappingTableId, sap.ui.getCore().byId("schemaVersionSelect"))
    },

    tabSelect: function (oEv) {
      if (oEv.getParameter("selectedKey") === "runs") {
        this.fetchRuns();
      }
    },

    onRemovePress: function (oEv) {
      let currentDataset = this._model.getProperty("/currentDataset");

      sap.m.MessageBox.show("This action will remove all versions of the dataset definition. \nAre you sure?", {
        icon: sap.m.MessageBox.Icon.WARNING,
        title: "Are you sure?",
        actions: [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO],
        onClose: (oAction) => {
          if (oAction === "YES") {
            this._datasetService.disable(currentDataset.name)
          }
        }
      });
    },

    routeMatched: function (oParams) {
      if (Prop.get(oParams, "id") === undefined) {
        this._datasetService.getTop().then(() => this.load())
      } else if (Prop.get(oParams, "version") === undefined) {
        this._datasetService.getLatestByName(oParams.id).then(() => this.load())
      } else {
        this._datasetService.getByNameAndVersion(oParams.id, oParams.version).then(() => this.load())
      }
      this.byId("datasetIconTabBar").setSelectedKey("info");
    },

    load: function() {
      let currentDataset = this._model.getProperty("/currentDataset");
      this.byId("info").setModel(new sap.ui.model.json.JSONModel(currentDataset), "dataset");
      this.fetchSchema();
      const auditTable = this.byId("auditTrailTable");
      if(currentDataset) {
        this._datasetService.getAuditTrail(currentDataset.name, auditTable);
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
    
    onScheduleEditPress: function(oEv) {
      this._editScheduleDialog.open();
      this._validator.clearAll(this._editScheduleDialog);

      const oCurrentDataset = this._model.getProperty("/currentDataset");
      
      const oAuditModel = this.byId("auditTrailTable").getModel("auditTrail");
      const aAuditEntries = oAuditModel.getProperty("/entries").map(e => {
        return {
          menasRef: e.menasRef
        };
      })
      aAuditEntries.unshift({
        menasRef: {
          name: oCurrentDataset.name,
          description: "Version created by this update",
          version: oCurrentDataset.version + 1
        }
      });
      
      this._editScheduleDialog.setModel(new sap.ui.model.json.JSONModel({entries: aAuditEntries}), "versions");
      
      const currSchedule = this._model.getProperty("/currentDataset/schedule");
      if(currSchedule) {
        this._model.setProperty("/newSchedule", jQuery.extend(true, {}, currSchedule));
      } else {
        this._model.setProperty("/newSchedule", {scheduleTiming: {}, runtimeParams: {}, rawFormat: {}});
      }
    },
    
    onScheduleSave: function() {
      const newSchedule = this._model.getProperty("/newSchedule")
      if(!this._validator.validate(this._editScheduleDialog)) {
        MessageToast.show("Please correct highlighted errors");
      } else {
        const oSchedule = this._model.getProperty("/newSchedule");
        let oDataset = this._model.getProperty("/currentDataset");
        oDataset.schedule = oSchedule;
        
        this._datasetService.update(oDataset);
        
        this._editScheduleDialog.setBusy(true).setBusyIndicatorDelay(0);
      }
    },
    
    closeScheduleDialog: function() {
      this._editScheduleDialog.close();
    }

  });
});
