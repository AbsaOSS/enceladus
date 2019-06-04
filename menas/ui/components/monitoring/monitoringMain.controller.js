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

 // module 'Something' wants to use third party library 'URI.js'
 // It is packaged by UI5 as non-UI5-module 'sap/ui/thirdparty/URI'
 // the following shim helps UI5 to correctly load URI.js and to retrieve the module's export value
 // Apps don't have to define that shim, it is already applied by ui5loader-autconfig.js
 sap.ui.define([
   "sap/ui/core/mvc/Controller",
   "sap/ui/unified/DateRange",
   "./../external/it/designfuture/chartjs/library-preload"
 ], function (Controller, DateRange, Openui5Chartjs) {
   "use strict";

   return Controller.extend("components.monitoring.monitoringMain", {
      oFormatYyyymmdd: null,

      /**
       * Called when a controller is instantiated and its View controls (if
       * available) are already created. Can be used to modify the View before it
       * is displayed, to bind event handlers and do other one-time
       * initialization.
       *
       * @memberOf components.monitoring.monitoringMain
       */
      onInit: function () {
        this._model = sap.ui.getCore().getModel();
        this._router = sap.ui.core.UIComponent.getRouterFor(this);
        this._router.getRoute("monitoring").attachMatched(function (oEvent) {
          //let arguments = oEvent.getParameter("arguments");
          this.routeMatched(arguments);
        }, this);
        this.oFormatYyyymmdd = sap.ui.core.format.DateFormat.getInstance({pattern: "yyyy-MM-dd", calendarType: sap.ui.core.CalendarType.Gregorian});
        this.setDefaultDateInterval()

        // set spanning headers for the table
        let oView = this.getView();
        oView.byId("multiheader-info").setHeaderSpan([2,1]);
        oView.byId("multiheader-checkpoint").setHeaderSpan([3,1]);
        oView.byId("multiheader-recordcount-raw").setHeaderSpan([5,1,1]);
        oView.byId("multiheader-recordcount-std").setHeaderSpan([0,2,1]);
        oView.byId("multiheader-recordcount-cnfrm").setHeaderSpan([0,2,1]);
      },

      // TODO: remove
      _formFragments: {},

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


      // remains
      datasetSelected: function (oEv) {
        let selected = oEv.getParameter("listItem").data("id");
        this._router.navTo("monitoring", {
          id: selected
        });
      },

      // remains
      routeMatched: function (oParams) {
        if (Prop.get(oParams, "id") === undefined) {
          MonitoringService.getDatasetList(true);
        } else {
          MonitoringService.getDatasetList();
          //DatasetService.getLatestDatasetVersion(oParams.id)
          this._model.setProperty("/datasetId", oParams.id)
          this.updateMonitoringData()
          //MonitoringService.getMonitoringPoints(oParams.id);
          //MonitoringService.getDatasetCheckpoints(oParams.id);
        }
      },

      // Calendar

      handleCalendarSelect: function(oEvent) {
        var oCalendar = oEvent.getSource();
        this._updateTimeInterval(oCalendar.getSelectedDates()[0]);
      },

      _updateTimeInterval: function(oSelectedDates) {
        let oDate;
        if (oSelectedDates) {
          oDate = oSelectedDates.getStartDate();
          if (oDate) {
            this._model.setProperty("/dateFrom", this.oFormatYyyymmdd.format(oDate))
          }
          oDate = oSelectedDates.getEndDate();
          if (oDate) {
            this._model.setProperty("/dateTo", this.oFormatYyyymmdd.format(oDate))
          }
        }
        this.updateMonitoringData()

      },

      updateMonitoringData: function () {
        let dateFrom = this._model.getProperty("/dateFrom")
        let dateTo = this._model.getProperty("/dateTo")
        let datasetId = this._model.getProperty("/datasetId")
        if (dateFrom != undefined && dateTo != undefined && datasetId != undefined) {
          MonitoringService.getData(datasetId, dateFrom, dateTo);
        } else {
          MonitoringService.clearMonitoringModel()
        }
      },

      // remove ?
      handleWeekNumberSelect: function(oEvent) {
        var oDateRange = oEvent.getParameter("weekDays");
        // var iWeekNumber = oEvent.getParameter("weekNumber");
        this._updateTimeInterval(oDateRange);
      },

      _selectWeekInterval: function(iDays) {
        var oCurrent = new Date();     // get current date
        var iWeekstart = oCurrent.getDate() - oCurrent.getDay() + 1;
        var iWeekend = iWeekstart + iDays;       // end day is the first day + 6
        var oMonday = new Date(oCurrent.setDate(iWeekstart));
        var oSunday = new Date(oCurrent.setDate(iWeekend));

        var oCalendar = this.byId("calendar");

        oCalendar.removeAllSelectedDates();
        oCalendar.addSelectedDate(new DateRange({startDate: oMonday, endDate: oSunday}));

        this._updateTimeInterval(oCalendar.getSelectedDates()[0]);
      },

      setDefaultDateInterval: function () {
        let oEnd = new Date()
        // Two weeks before today
        let oStart = new Date(oEnd.getTime() - 14 * 24 * 60 * 60 * 1000);

        let oCalendar = this.byId("calendar");
        oCalendar.removeAllSelectedDates();
        oCalendar.addSelectedDate(new sap.ui.unified.DateRange({startDate: oStart, endDate: oEnd}))
        this._updateTimeInterval(oCalendar.getSelectedDates()[0]);
      }

   });
 },/* bExport= */ true);
