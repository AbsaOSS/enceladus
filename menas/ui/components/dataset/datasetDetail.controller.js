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
  "components/types/CronEntryType",
  "components/validator/Validator",
  "sap/m/MessageToast",
  "./../external/it/designfuture/chartjs/library-preload",
  "components/tables/TableUtils"
], function (Controller, Fragment, CronEntryType, Validator, MessageToast, Openui5Chartjs, TableUtils) {
  "use strict";

  return Controller.extend("components.dataset.datasetDetail", {

    /**
     * Called when a controller is instantiated and its View controls (if
     * available) are already created. Can be used to modify the View before it
     * is displayed, to bind event handlers and do other one-time
     * initialization.
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

      // Monitoring

      this.MAX_RUNS = 500;
      this.byId("dateFromPicker").setDisplayFormat(Formatters.infoDatePattern);
      this.byId("dateToPicker").setDisplayFormat(Formatters.infoDatePattern);
      this.setDefaultMonitoringDateInterval();
      this._model.setProperty("/monitoringTotalsVisible", true);
      this._model.setProperty("/monitoringRecordsVisible", true);
      this._model.setProperty("/monitoringDetailsVisible", false);

      this._upsertConformanceRuleDialog = Fragment.load({
        id: view.getId(),
        name: "components.dataset.conformanceRule.upsert",
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

      this._rb = sap.ui.getCore().getModel("i18n").getResourceBundle();

      // Cron time picker
      let cronTemplate = {
        "minute" : this._generateCronTemplateRange(0, 60, this._rb),
        "hour": this._generateCronTemplateRange(0, 24, this._rb),
        "dayOfMonth": this._generateCronTemplateRange(1, 32, this._rb),
        "month": this._generateCronTemplateRange(1, 13, this._rb, "MENAS_SCHEDULE_MONTH"),
        "dayOfWeek": this._generateCronTemplateRange(0, 7, this._rb, "MENAS_SCHEDULE_DAY")
      }
      this._model.setProperty("/cronFormTemplate", cronTemplate);
      
      const auditTable = this.byId("auditTrailTable");
      const auditTableUtils = new TableUtils(auditTable, "Audit Trail");
      auditTableUtils.makeSortable(["Change Time", "Author", "Version"], 
          ["updated", "updatedBy", "menasRef/version"]);
      auditTableUtils.makeGroupable(["Author"], ["updatedBy"]);
    },

    _generateCronTemplateRange: function(iStart, iEnd, oRb, oRbProperty) {
      return ["*", ...(_.range(iStart, iEnd, 1))].map(n => {
        let description = n.toString();
        if(oRbProperty) {
          description = oRb.getText(`${oRbProperty}[${n}]`);
        } else if(n === "*") {
          description = oRb.getText("MENAS_SCHEDULE_ANY");
        }
        return {
        "key": n.toString(),
        "name": description
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
      this._addRule()
    },

    _setRuleDialogModel: function (rules) {
      const ruleIndex = rules.length;
      const schema = this._transitiveSchemas[ruleIndex];
      const model = new sap.ui.model.json.JSONModel(schema);
      model.setSizeLimit(5000);
      this._upsertConformanceRuleDialog.setModel(model, "schema");
    },

    onRuleMenuAction: function (oEv) {
      let sAction = oEv.getParameter("item").data("action");
      let sBindPath = oEv.getParameter("item").getBindingContext().getPath();

      switch (sAction) {
        case "edit":
          this.editRule(sBindPath);
          break;
        case "delete":
          this.deleteRule(sBindPath);
          break;
        case "moveUp":
          this.moveRuleUp(sBindPath);
          break;
        case "moveDown":
          this.moveRuleDown(sBindPath);
          break;
        case "addBefore":
          this.addRuleBefore(sBindPath);
          break;
        case "addAfter":
          this.addRuleAfter(sBindPath);
          break;
        default:
          break;
      }
    },

    editRule: function(sBindPath) {
      let old = this._model.getProperty(sBindPath);
      this._model.setProperty("/newRule", {
        ...$.extend(true, {}, old),
        title: "Edit",
        isEdit: true,
      });

      const rules = this._model.getProperty("/currentDataset/conformance").slice(0, old.order);
      this._setRuleDialogModel(rules);
      this._upsertConformanceRuleDialog.open();
    },

    deleteRule: function(sBindPath) {
      sap.m.MessageBox.confirm("Are you sure you want to delete the conformance rule?", {
        actions: [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO],
        onClose: function (oResponse) {
          if (oResponse === sap.m.MessageBox.Action.YES) {
            let ruleIndex = this._getRuleIndex(sBindPath);
            let currentDataset = this._model.getProperty("/currentDataset");
            new SchemaRestDAO().getByNameAndVersion(currentDataset.schemaName, currentDataset.schemaVersion).then(schema => {
              let result = RuleService.removeRule(currentDataset.conformance, schema, ruleIndex);

              if (result.isValid) {
                const newDataset = {...currentDataset, conformance: result.result};
                this._datasetService.update(newDataset);
              } else {
                sap.m.MessageToast.show(result.errorMessage, {width: "20em"});
              }
            });
          }
        }.bind(this)
      });
    },

    moveRuleUp: function(sBindPath) {
      this._moveRule(sBindPath, RuleService.moveRuleUp)
    },

    moveRuleDown: function(sBindPath) {
      this._moveRule(sBindPath, RuleService.moveRuleDown)
    },

    _moveRule: function(sBindPath, moveRuleCallback) {
      const ruleIndex = this._getRuleIndex(sBindPath);
      const currentDataset = this._model.getProperty("/currentDataset");

      new SchemaRestDAO().getByNameAndVersion(currentDataset.schemaName, currentDataset.schemaVersion).then(schema => {
        const result = moveRuleCallback(currentDataset.conformance, schema, ruleIndex);
        if (result.isValid) {
          const newDataset = {...currentDataset, conformance: result.result};
          this._datasetService.update(newDataset);
        } else {
          sap.m.MessageToast.show(result.errorMessage, { width: "20em" });
        }
      });
    },

    addRuleBefore: function(sBindPath) {
      const ruleIndex = this._getRuleIndex(sBindPath);
      this._addRule(ruleIndex);
    },

    addRuleAfter: function(sBindPath) {
      const ruleIndex = this._getRuleIndex(sBindPath);
      this._addRule(ruleIndex + 1);
    },

    _addRule: function(index) {
      this._model.setProperty("/newRule", {
        title: "Add",
        isEdit: false,
        order: index
      });

      const rules = this._model.getProperty("/currentDataset/conformance").slice(0, index);
      this._upsertConformanceRuleDialog.open();

      this._setRuleDialogModel(rules)
    },

    _getRuleIndex: function(sBindPath) {
      let toks = sBindPath.split("/");
      return parseInt(toks[toks.length - 1]);
    },

    auditVersionPress: function (oEv) {
      let oSrc = oEv.getSource();
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

    fetchRuns: function () {
      let currentDataset = this._model.getProperty("/currentDataset");
      RunService.getDatasetRuns(this.byId("Runs"), currentDataset.name, currentDataset.version);
    },

    mappingTableSelect: function (oEv) {
      let sMappingTableId = oEv.getParameter("selectedItem").getKey();
      this._mappingTableService.getAllVersions(sMappingTableId, sap.ui.getCore().byId("schemaVersionSelect"))
    },

    tabSelect: function (oEv) {
      switch (oEv.getParameter("selectedKey")) {
        case "runs":
          this.fetchRuns();
          break;
        case "monitoring":
          this.updateMonitoringData();
          break;
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
      if(currentDataset) {
        this._transitiveSchemas = [];
        const transitiveSchemas = this._transitiveSchemas;
        new SchemaRestDAO().getByNameAndVersionSync(currentDataset.schemaName, currentDataset.schemaVersion).then((schema) => {
          this._model.setProperty("/currentDataset/schema", schema)
          this._schemaTable.model = schema;
          transitiveSchemas.push(schema);
          SchemaManager.getTransitiveSchemas(transitiveSchemas, currentDataset.conformance)
        });

        const auditTable = this.byId("auditTrailTable");
        this._datasetService.getAuditTrail(currentDataset.name, auditTable);
      }
    },

    conformanceRuleFactory: function (sId, oContext) {
      const lastIndex = this._model.getProperty("/currentDataset/conformance").length - 1;
      const currentIndex = oContext.getProperty("order");
      if (currentIndex === 0) {
        this._model.setProperty(`/currentDataset/conformance/${currentIndex}/isFirst`, true);
      }
      if (currentIndex === lastIndex) {
        this._model.setProperty(`/currentDataset/conformance/${currentIndex}/isLast`, true);
      }

      let sFragmentName = "components.dataset.conformanceRule." + oContext.getProperty("_t") + ".display";
      if (oContext.getProperty("_t") === "MappingConformanceRule") {

        let oAttributeMappings = oContext.getProperty("attributeMappings");
        let aJoinConditions = [];
        for (let key in oAttributeMappings) {
          aJoinConditions.push({
            mappingTableField: key,
            datasetField: oAttributeMappings[key],
            datasetName: this._model.getProperty("/currentDataset/name"),
            mappingTableName: this._model.getProperty("/currentDataset/name")
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
        this._model.setProperty("/newSchedule", jQuery.extend(true, {}, this._model.getProperty("/newScheduleDefault")));
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
    },

    // Monitoring related part

    // Processes parsing errors
    handleDateChange: function(oEvent) {
      let oDP = oEvent.getSource();
      let bValid = oEvent.getParameter("valid");
      if (bValid) {
        oDP.setValueState(sap.ui.core.ValueState.None);
      } else {
        oDP.setValueState(sap.ui.core.ValueState.Error);
      }
    },

    updateMonitoringData: function () {
      let oMonitoringDateFrom = this._model.getProperty("/monitoringDateFrom");
      let oMonitoringDateTo = this._model.getProperty("/monitoringDateTo");
      let intervalDays = 1 + Math.floor((oMonitoringDateTo - oMonitoringDateFrom) / (1000 * 60 * 60 * 24));
      this._model.setProperty("/monitoringIntervalDays", intervalDays);

      let sMonitoringDateFrom = Formatters.toStringInfoDate(oMonitoringDateFrom);
      let sMonitoringDateTo = Formatters.toStringInfoDate(oMonitoringDateTo);
      let datasetName = this._model.getProperty("/currentDataset/name");
      if (sMonitoringDateFrom != undefined && sMonitoringDateTo != undefined && datasetName != undefined) {
        MonitoringService.getData(datasetName, sMonitoringDateFrom, sMonitoringDateTo)
          .then(k => {this._checkRunDataOverflow()});
      } else {
        MonitoringService.clearMonitoringModel();
      }
    },

    _checkRunDataOverflow: function () {
      if (this._model.getProperty("/numberOfPoints") >= this.MAX_RUNS) {
        sap.m.MessageBox.show("The number of found runs exceeds the display limit of " + this.MAX_RUNS +
          ". Some of the runs are not shown. Please, select a smaller date interval and retry.", {
          icon: sap.m.MessageBox.Icon.WARNING,
          title: "Too large date interval",
          actions: [sap.m.MessageBox.Action.OK]
        });
      }
    },

    onFindRunsPress: function (oEv){
      // Check selected interval
      this.updateMonitoringData()
    },

    setDefaultMonitoringDateInterval: function () {
      let oEnd = new Date();
      let oStart = new Date(oEnd.getTime() - 14 * 24 * 60 * 60 * 1000); // Two weeks before today
      this._model.setProperty("/monitoringDateFrom", oStart);
      this._model.setProperty("/monitoringDateTo", oEnd)
    },

    monitoringToRun: function (oEv) {
      let oRow = oEv.getParameter("row");
      let datasetName = this._model.getProperty("datasetName",oRow.getBindingContext());
      let datasetVersion = this._model.getProperty("datasetVersion",oRow.getBindingContext());
      let runId = this._model.getProperty("runId",oRow.getBindingContext());

      this._router.navTo("runs", {
        dataset: datasetName,
        version: datasetVersion,
        id: runId
      });
    }

  });
});
