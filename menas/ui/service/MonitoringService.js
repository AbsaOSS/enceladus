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

var MonitoringService = new function() {

  let model = sap.ui.getCore().getModel();

  let barChartLabels = []; // run info-date + info-version

  let runStatusAggregator = {
    "allSucceeded" : {infoLabel: 8, color: "green", counter: 0},
    "stageSucceeded" : {infoLabel: 5, color: "blue", counter: 0},
    "running" : {infoLabel: 7, color: "teal", counter: 0},
    "failed" : {infoLabel: 3, color: "red", counter: 0},
    "other" : {infoLabel: 4, color: "magenta", counter: 0}
  };

  let recordsStatusAggregator = {
    "Conformed" : {color: "green", counter: 0, recordCounts: []},
    "Failed at conformance" : {color: "red", counter: 0, recordCounts: []},
    "Standardized (not conformed)" : {color: "blue", counter: 0, recordCounts: []},
    "Failed at standardization" : {color: "orange", counter: 0, recordCounts: []},
    "Unprocessed raw" : {color: "grey", counter: 0, recordCounts: []},
    "Inconsistent info" : {color: "magenta", counter: 0, recordCounts: []}
  };

  this.clearAggregators = function() {
    barChartLabels = [];

    // clear runStatusAggregator counters
    Object.keys(runStatusAggregator).forEach(function(key) {
      runStatusAggregator[key].counter = 0
    });

    // clear recordsStatusAggregator counters and arrays of data
    Object.keys(recordsStatusAggregator).forEach(function(key) {
      recordsStatusAggregator[key].counter = 0;
      recordsStatusAggregator[key].recordCounts = []
    });
  };

  this.processRunStatus = function(oRun) {
    let status = oRun["status"];
    if (runStatusAggregator.hasOwnProperty(status)) {
      runStatusAggregator[status].counter += 1;
      oRun["infoLabel"] = runStatusAggregator[status].infoLabel
    } else {
      runStatusAggregator["other"].counter += 1;
      oRun["infoLabel"] = runStatusAggregator["other"].infoLabel
    }
  };

  this.clearMonitoringModel = function() {
    model.setProperty("/barChartHeight", "10rem");
    model.setProperty("/numberOfPoints", 0);
    model.setProperty("/monitoringRunData", []);
    model.setProperty("/barChartData", {
      labels: [],
      datasets: []
    });
    model.setProperty("/pieChartRecordTotals", {
      labels: [],
      datasets: []
    });
    model.setProperty("/pieChartStatusTotals", {
      labels: [],
      datasets: []
    });
  };

  this.setMonitoringModel = function(oData){

    // prepare data for PIE chart of RUN statuses
    let runStatusLabels = Object.keys(runStatusAggregator);
    let pieChartStatusTotals = {
      labels: runStatusLabels,
      datasets: [
        {
          data: runStatusLabels.map(x => runStatusAggregator[x].counter),
          backgroundColor: runStatusLabels.map(x => runStatusAggregator[x].color)
        }]
    };

    // prepare data for PIE chart of statuses of RECORDS
    let recordStatusLabels = Object.keys(recordsStatusAggregator);
    let pieChartRecordTotals = {
      labels: recordStatusLabels,
      datasets: [
        {
          data: recordStatusLabels.map(x => recordsStatusAggregator[x].counter),
          backgroundColor: recordStatusLabels.map(x => recordsStatusAggregator[x].color)
        }]

    };

    // prepare data for BAR chart of RECORDS per run
    let datasets = [];
    recordStatusLabels.map(x => datasets.push({
      label : x,
      backgroundColor : recordsStatusAggregator[x].color,
      data : recordsStatusAggregator[x].recordCounts,
      stack : "all"}));
    let barChartData = {
      labels: barChartLabels,
      datasets: datasets
      };

    // set layout properties

    model.setProperty("/numberOfPoints", oData.length);
    model.setProperty("/barChartHeight", 10 + 2 * barChartLabels.length + "rem");

    // set model
    model.setProperty("/monitoringRunData", oData);
    model.setProperty("/barChartData", barChartData);
    model.setProperty("/pieChartRecordTotals", pieChartRecordTotals);
    model.setProperty("/pieChartStatusTotals", pieChartStatusTotals);
  };

  this.processRecordCounts = function(oRun) {
    barChartLabels.push(oRun["informationDate"] + " v" + oRun["reportVersion"]);
    if (MonitoringService.conformanceFinishedCorrectly(oRun)) {
      MonitoringService.processConformed(oRun);
      return;
    }else if (MonitoringService.standardizationFinishedCorrectly(oRun)) {
      MonitoringService.processStandardized(oRun);
      return;
    }else if (MonitoringService.rawRegisteredCorrectly(oRun)){
      MonitoringService.processRaw(oRun);
      return;
    } else {
      MonitoringService.processInconsistent(oRun);
    }
  };

  this.conformanceFinishedCorrectly = function(oRun) {
    return (!isNaN(oRun["raw_recordcount"])
      && !isNaN(oRun["std_records_succeeded"]) && !isNaN(oRun["std_records_failed"])
      && !isNaN(oRun["conform_records_succeeded"]) && !isNaN(oRun["conform_records_failed"])
      // sanity checks
      && (+oRun["std_records_succeeded"]) + (+oRun["std_records_failed"]) == (+oRun["raw_recordcount"])
      && (+oRun["conform_records_succeeded"]) + (+oRun["conform_records_failed"]) == (+oRun["raw_recordcount"])
      && (+oRun["std_records_succeeded"]) >= (+oRun["conform_records_succeeded"] ))
  };


  this.standardizationFinishedCorrectly = function(oRun) {
    return (!isNaN(oRun["raw_recordcount"]) && !isNaN(oRun["raw_recordcount"])
      && !isNaN(oRun["std_records_succeeded"]) && !isNaN(oRun["std_records_failed"])
      && oRun["conform_records_succeeded"] == undefined && oRun["conform_records_failed"] == undefined
      //sanity checks
      && (+oRun["std_records_succeeded"]) + (+oRun["std_records_failed"]) == (+oRun["raw_recordcount"]) )
  };

  this.rawRegisteredCorrectly = function(oRun) {
    return (!isNaN(oRun["raw_recordcount"]) && !isNaN(oRun["raw_recordcount"])
      && oRun["std_records_succeeded"]  == undefined && oRun["std_records_failed"] == undefined
      && oRun["conform_records_succeeded"]  == undefined && oRun["conform_records_failed"] == undefined)
  };

  this.processConformed = function(oRun) {
    recordsStatusAggregator["Conformed"].recordCounts.push(+oRun["conform_records_succeeded"]);
    recordsStatusAggregator["Failed at conformance"].recordCounts.push(+oRun["conform_records_failed"] - +oRun["std_records_failed"]);
    recordsStatusAggregator["Standardized (not conformed)"].recordCounts.push(0);
    recordsStatusAggregator["Failed at standardization"].recordCounts.push(+oRun["std_records_failed"]);
    recordsStatusAggregator["Unprocessed raw"].recordCounts.push(0);
    recordsStatusAggregator["Inconsistent info"].recordCounts.push(0);

    recordsStatusAggregator["Conformed"].counter += +oRun["conform_records_succeeded"];
    recordsStatusAggregator["Failed at conformance"].counter += +oRun["conform_records_failed"] - +oRun["std_records_failed"];
    recordsStatusAggregator["Failed at standardization"].counter += +oRun["std_records_failed"]
  };

  this.processStandardized = function(oRun) {
    recordsStatusAggregator["Conformed"].recordCounts.push(0);
    recordsStatusAggregator["Failed at conformance"].recordCounts.push(0);
    recordsStatusAggregator["Standardized (not conformed)"].recordCounts.push(+oRun["std_records_succeeded"]);
    recordsStatusAggregator["Failed at standardization"].recordCounts.push(+oRun["std_records_failed"]);
    recordsStatusAggregator["Unprocessed raw"].recordCounts.push(0);
    recordsStatusAggregator["Inconsistent info"].recordCounts.push(0);

    recordsStatusAggregator["Standardized (not conformed)"].counter += +oRun["std_records_succeeded"];
    recordsStatusAggregator["Failed at standardization"].counter += +oRun["std_records_failed"]
  };

  this.processRaw = function(oRun) {
    recordsStatusAggregator["Conformed"].recordCounts.push(0);
    recordsStatusAggregator["Failed at conformance"].recordCounts.push(0);
    recordsStatusAggregator["Standardized (not conformed)"].recordCounts.push(0);
    recordsStatusAggregator["Failed at standardization"].recordCounts.push(0);
    recordsStatusAggregator["Unprocessed raw"].recordCounts.push(+oRun["raw_recordcount"]);
    recordsStatusAggregator["Inconsistent info"].recordCounts.push(0);

    recordsStatusAggregator["Unprocessed raw"].counter += +oRun["raw_recordcount"]
  };

  this.processInconsistent = function(oRun) {
    recordsStatusAggregator["Conformed"].recordCounts.push(0);
    recordsStatusAggregator["Failed at conformance"].recordCounts.push(0);
    recordsStatusAggregator["Standardized (not conformed)"].recordCounts.push(0);
    recordsStatusAggregator["Failed at standardization"].recordCounts.push(0);
    recordsStatusAggregator["Unprocessed raw"].recordCounts.push(0);

    // try to estimate number of records involved in this run
    let estimatedRecordCount = 0;
    if (!isNaN(oRun["raw_recordcount"])) {
      estimatedRecordCount = +oRun["raw_recordcount"]
    } else if (!isNaN(oRun["std_records_succeeded"])&& !isNaN(oRun["std_records_failed"])){
      estimatedRecordCount = (+oRun["std_records_succeeded"]) + (+oRun["std_records_failed"])
    } else if (!isNaN(oRun["conform_records_succeeded"]) && !isNaN(oRun["conform_records_failed"])) {
      estimatedRecordCount = (+oRun["conform_records_succeeded"]) + (+oRun["conform_records_failed"] )
    }
    recordsStatusAggregator["Inconsistent info"].recordCounts.push(estimatedRecordCount);
    recordsStatusAggregator["Inconsistent info"].counter += estimatedRecordCount
  };


  this.getData= function(sId, sStartDate, sEndDate) {
    MonitoringService.clearAggregators();
    Functions.ajax("api/monitoring/data/datasets/"
      + encodeURI(sId) + "/"
      + encodeURI(sStartDate) + "/"
      + encodeURI(sEndDate),
      "GET", {}, function(oData) {

      if (oData.length > 0) {
        for (let oRun of oData) {
          MonitoringService.processRunStatus(oRun);
          MonitoringService.processRecordCounts(oRun)
        }

        MonitoringService.setMonitoringModel(oData)

      } else {
        // no datapoints for this interval
        MonitoringService.clearMonitoringModel()
      }
    }, function() {
        MonitoringService.clearMonitoringModel();
      sap.m.MessageBox
        .error("Failed to get monitoring points. Please wait a moment and try reloading the application")
    })
  };

}();
