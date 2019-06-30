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

  // time in milliseconds since last checkpoint to detect zombie
  let runningJobExpirationThreshold = 1000 * 60 * 60 * 24;

  let warningTypes = {
    inconsistentInfo: {infoLabel: 4, state: "None", icon: "sap-icon://question-mark", text: "Inconsistent run info"},
    zombie: {infoLabel: 9, state: "None", icon: "sap-icon://lateness", text: "Zombie job"},
    stdErrors: {infoLabel: 1, state: "Warning", icon: "sap-icon://message-warning", text: "Errors at standardization"},
    cnfrmErrors: {infoLabel: 3, state: "Error", icon: "sap-icon://message-warning", text: "Errors at conformance"},
    noSucceded: {infoLabel: 3, state: "Error", icon: "sap-icon://alert", text: "No records succeeded"}
  };

  let totals = {
    "records": 0,
    "rawDirSize": 0,
    "stdDirSize": 0,
    "publishDirSize": 0
  };

  let runStatusAggregator = {
    "allSucceeded" : {infoLabel: 8, color: "green", counter: 0},
    "stageSucceeded" : {infoLabel: 5, color: "blue", counter: 0},
    "running" : {infoLabel: 7, color: "teal", counter: 0},
    "failed" : {infoLabel: 3, color: "red", counter: 0},
    "Оther" : {infoLabel: 4, color: "magenta", counter: 0}
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

    // clear totals
    Object.keys(totals).forEach(function(key) {
      totals[key] = 0;
    });

    // clear runStatusAggregator counters
    Object.keys(runStatusAggregator).forEach(function(key) {
      runStatusAggregator[key].counter = 0;
    });

    // clear recordsStatusAggregator counters and arrays of data
    Object.keys(recordsStatusAggregator).forEach(function(key) {
      recordsStatusAggregator[key].counter = 0;
      recordsStatusAggregator[key].recordCounts = []
    });
  };

  this.processDirSizes = function(oRun) {
    let rawDirSize = oRun["controlMeasure"]["metadata"]["additionalInfo"]["std_input_dir_size"];
    let stdDirSize = oRun["controlMeasure"]["metadata"]["additionalInfo"]["std_output_dir_size"];
    let publishDirSize = oRun["controlMeasure"]["metadata"]["additionalInfo"]["conform_output_dir_size"];
    if (!isNaN(rawDirSize)) {totals["rawDirSize"] += +rawDirSize}
    if (!isNaN(stdDirSize)) {totals["stdDirSize"] += +stdDirSize}
    if (!isNaN(publishDirSize)) {totals["publishDirSize"] += +publishDirSize}
  }

  this.processRunStatus = function(oRun) {
    let status = oRun["status"];
    oRun["prettyStatus"] = Formatters.statusToPrettyString(status);
    if (runStatusAggregator.hasOwnProperty(status)) {
      runStatusAggregator[status].counter += 1;
      oRun["infoLabel"] = runStatusAggregator[status].infoLabel;
      MonitoringService.checkZombieJob(oRun);
    } else {
      runStatusAggregator["Оther"].counter += 1;
      oRun["infoLabel"] = runStatusAggregator["Оther"].infoLabel
    }
  };

  this.checkZombieJob = function(oRun) {
    if (oRun["status"] == "running"
        && (new Date() - oRun["latestCheckpoint"]["endDate"]) > runningJobExpirationThreshold ){
      oRun["warnings"].push(warningTypes.zombie);
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

    // prepare total recordcount
    Object.keys(recordsStatusAggregator).forEach(function(key) {
      totals["records"] += recordsStatusAggregator[key].counter;
    });

    // prepare data for PIE chart of RUN statuses
    let runStatusKeys = Object.keys(runStatusAggregator);
    let runStatusLabels = runStatusKeys.map(sStatus => Formatters.statusToPrettyString(sStatus));
    let pieChartStatusTotals = {
      labels: runStatusLabels,
      datasets: [
        {
          data: runStatusKeys.map(x => runStatusAggregator[x].counter),
          backgroundColor: runStatusKeys.map(x => runStatusAggregator[x].color)
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
    model.setProperty("/barChartHeight", Math.max(15, 2 * barChartLabels.length) + "rem");

    // set model
    model.setProperty("/monitoringRunData", oData);
    model.setProperty("/barChartData", barChartData);
    model.setProperty("/pieChartRecordTotals", pieChartRecordTotals);
    model.setProperty("/pieChartStatusTotals", pieChartStatusTotals);
    model.setProperty("/monitoringTotals", totals);
  };

  this.processRecordCounts = function(oRun) {
    barChartLabels.push(oRun["infoDateString"] + " v" + oRun["reportVersion"]);
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
    return !isNaN(oRun["rawRecordcount"])
      && oRun["std_records_succeeded"] != null && oRun["std_records_failed"] != null
      && oRun["conform_records_succeeded"] != null && oRun["conform_records_failed"] != null
      // sanity checks
      && (+oRun["std_records_succeeded"]) + (+oRun["std_records_failed"]) == (+oRun["rawRecordcount"])
      && (+oRun["conform_records_succeeded"]) + (+oRun["conform_records_failed"]) == (+oRun["rawRecordcount"])
      && (+oRun["std_records_succeeded"]) >= (+oRun["conform_records_succeeded"] )
  };


  this.standardizationFinishedCorrectly = function(oRun) {
    return !isNaN(oRun["rawRecordcount"])
      && oRun["std_records_succeeded"] != null && oRun["std_records_failed"] != null
      && oRun["conform_records_succeeded"] == null && oRun["conform_records_failed"] == null
      //sanity checks
      && (+oRun["std_records_succeeded"]) + (+oRun["std_records_failed"]) == (+oRun["rawRecordcount"])
  };

  this.rawRegisteredCorrectly = function(oRun) {
    return !isNaN(oRun["rawRecordcount"])
      && oRun["std_records_succeeded"]  == null && oRun["std_records_failed"] == null
      && oRun["conform_records_succeeded"]  == null && oRun["conform_records_failed"] == null
  };

  this.processConformed = function(oRun) {
    let succeeded = +oRun["conform_records_succeeded"];
    recordsStatusAggregator["Conformed"].recordCounts.push(succeeded);
    if (succeeded == 0) {oRun["warnings"].push(warningTypes.noSucceded)};

    let failedAtConformance = +oRun["conform_records_failed"] - +oRun["std_records_failed"]
    recordsStatusAggregator["Failed at conformance"].recordCounts.push(failedAtConformance);
    if (failedAtConformance > 0) {oRun["warnings"].push(warningTypes.cnfrmErrors)};

    recordsStatusAggregator["Standardized (not conformed)"].recordCounts.push(0);

    let failedAtStd = +oRun["std_records_failed"]
    recordsStatusAggregator["Failed at standardization"].recordCounts.push(failedAtStd);
    if (failedAtStd > 0) {oRun["warnings"].push(warningTypes.stdErrors)};

    recordsStatusAggregator["Unprocessed raw"].recordCounts.push(0);
    recordsStatusAggregator["Inconsistent info"].recordCounts.push(0);

    recordsStatusAggregator["Conformed"].counter += +oRun["conform_records_succeeded"];
    recordsStatusAggregator["Failed at conformance"].counter += +oRun["conform_records_failed"] - +oRun["std_records_failed"];
    recordsStatusAggregator["Failed at standardization"].counter += +oRun["std_records_failed"]
  };

  this.processStandardized = function(oRun) {
    recordsStatusAggregator["Conformed"].recordCounts.push(0);
    recordsStatusAggregator["Failed at conformance"].recordCounts.push(0);

    let succeeded = +oRun["std_records_succeeded"];
    recordsStatusAggregator["Standardized (not conformed)"].recordCounts.push(succeeded);
    if (succeeded == 0) {oRun["warnings"].push(warningTypes.noSucceded)};

    let failedAtStd = +oRun["std_records_failed"]
    recordsStatusAggregator["Failed at standardization"].recordCounts.push(failedAtStd);
    if (failedAtStd > 0) {oRun["warnings"].push(warningTypes.stdErrors)};

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
    recordsStatusAggregator["Unprocessed raw"].recordCounts.push(+oRun["rawRecordcount"]);
    recordsStatusAggregator["Inconsistent info"].recordCounts.push(0);

    recordsStatusAggregator["Unprocessed raw"].counter += +oRun["rawRecordcount"]
  };

  this.processInconsistent = function(oRun) {
    oRun["warnings"].push(warningTypes.inconsistentInfo);
    recordsStatusAggregator["Conformed"].recordCounts.push(0);
    recordsStatusAggregator["Failed at conformance"].recordCounts.push(0);
    recordsStatusAggregator["Standardized (not conformed)"].recordCounts.push(0);
    recordsStatusAggregator["Failed at standardization"].recordCounts.push(0);
    recordsStatusAggregator["Unprocessed raw"].recordCounts.push(0);

    // try to estimate number of records involved in this run
    let estimatedRecordCount = 0;
    if (!isNaN(oRun["rawRecordcount"] && +oRun["rawRecordcount"] > 0 )) {
      estimatedRecordCount = +oRun["rawRecordcount"]
    } else if (oRun["std_records_succeeded"] != null && oRun["std_records_failed"] != null){
      estimatedRecordCount = (+oRun["std_records_succeeded"]) + (+oRun["std_records_failed"])
    } else if (oRun["conform_records_succeeded"] != null && oRun["conform_records_failed"] != null) {
      estimatedRecordCount = (+oRun["conform_records_succeeded"]) + (+oRun["conform_records_failed"] )
    }
    recordsStatusAggregator["Inconsistent info"].recordCounts.push(estimatedRecordCount);
    recordsStatusAggregator["Inconsistent info"].counter += estimatedRecordCount
  };

  // find raw recordcount
  // TODO: Compare controlls across all checkpoints
  this.processCheckpoints = function(oRun) {
    let aCheckpoints = oRun["controlMeasure"]["checkpoints"];
    if ( !(Formatters.nonEmptyObject(aCheckpoints) && aCheckpoints.length > 0)) { return; }

    for (let oCheckpoint of aCheckpoints) {
      let controlNameLowerCase = oCheckpoint["name"].toLowerCase();
      if (controlNameLowerCase == "source" || controlNameLowerCase == "raw" ) {
        let aControls = oCheckpoint["controls"];
        if ( !(Formatters.nonEmptyObject(aControls) && aControls.length > 0)) { continue; }

        for (let oControl of aControls) {
          if (oControl["controlName"].toLowerCase() == "recordcount"
            && !isNaN(oControl["controlValue"])
            && +oControl["controlValue"] >= 0) {
              oRun["rawRecordcount"] = +oControl["controlValue"];
              return;
          }
        }

      }
    }

  };

  this.processLatestCheckpoint = function (oRun) {
    let aCheckpoints = oRun["controlMeasure"]["checkpoints"];
    if ( !(Formatters.nonEmptyObject(aCheckpoints) && aCheckpoints.length > 0)) { return; }
    let original = aCheckpoints[aCheckpoints.length -1];
    let latestCheckpoint = {
      name: original["name"],
      order: original["order"],
      workflowName: original["workflowName"],
      //startDate: moment(original["processStartTime"], "DD-MM-YYYY HH:mm:ss").toDate(),
      endDate: moment(original["processEndTime"], "DD-MM-YYYY HH:mm:ss").toDate()
    };
    oRun["latestCheckpoint"] = latestCheckpoint;
  };


  this.getData = function(sId, sStartDate, sEndDate) {
    MonitoringService.clearAggregators();
    return Functions.ajax("api/monitoring/data/datasets/"
      + encodeURI(sId) + "/"
      + encodeURI(sStartDate) + "/"
      + encodeURI(sEndDate),
      "GET", {}, function(oData) {

      if (oData.length > 0) {
        for (let oRun of oData) {
          oRun["infoDate"] = new Date(oRun["informationDateCasted"]["$date"]); // milliseconds to date
          oRun["infoDateString"] = Formatters.infoDateToString(oRun["infoDate"]);
          oRun["warnings"] = [];

          MonitoringService.processLatestCheckpoint(oRun);
          MonitoringService.processRunStatus(oRun);
          MonitoringService.processDirSizes(oRun);
          MonitoringService.processCheckpoints(oRun);
          MonitoringService.processRecordCounts(oRun);
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
