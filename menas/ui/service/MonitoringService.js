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

  var model = sap.ui.getCore().getModel();
  this.getMonitoringPoints = function(sId) {
    let sStartDate = model.getProperty("/dateFrom")
    let sEndDate = model.getProperty("/dateTo")
    Functions.ajax("api/monitoring/data/datasets/"
      + encodeURI(sId) + "/"
      + encodeURI(sStartDate) + "/"
      + encodeURI(sEndDate),
      "GET", {}, function(oData) {
      model.setProperty("/monitoringPoints", oData)
      let plotLabels = []

      let conformed = []
      let failedAfterConformance = []
      let waitingToConform = []
      let failedAfterStandardization = []
      let unprocessedRaw = []

      if (oData != undefined && oData.length != 0) {
        for (let elem of oData) {

          let rawTotal = elem["raw_recordcount"]
          let stdS = elem["std_records_succeeded"]
          let stdF = elem["std_records_failed"]
          let confS = elem["conform_records_succeeded"]
          let confF = elem["conform_records_failed"]

          if (rawTotal != undefined && stdS != undefined && stdF != undefined && confS != undefined && confF != undefined) {
            // conformance finished
            conformed.push(stdS)
            failedAfterConformance.push(confF - stdF)
            waitingToConform.push(0)
            failedAfterStandardization.push(stdF)
            unprocessedRaw.push(rawTotal - confS - confF)

            plotLabels.push(elem["informationDate"] + "_v" + elem["reportVersion"]+ " datasetVersion: " + elem["datasetVersion"])
            continue
          } else if (rawTotal != undefined && stdS != undefined && stdF != undefined && confS == undefined && confF == undefined) {
            // only standartization finished
            conformed.push(0)
            failedAfterConformance.push(0)
            waitingToConform.push(stdS)
            failedAfterStandardization.push(stdF)
            unprocessedRaw.push(rawTotal - stdS - stdF)


            plotLabels.push(elem["informationDate"] + "_v" + elem["reportVersion"] + " datasetVersion: " + elem["datasetVersion"])
            continue
          } else if (rawTotal != undefined && stdS == undefined && stdF == undefined && confS == undefined && confF == undefined) {
            // only raw data is available
            conformed.push(0)
            failedAfterConformance.push(0)
            waitingToConform.push(0)
            failedAfterStandardization.push(0)
            unprocessedRaw.push(rawTotal)

            plotLabels.push(elem["informationDate"] + "_v" + elem["reportVersion"]+  " datasetVersion: " + elem["datasetVersion"])
            continue
          } else {
            // run data is inconsistent
            plotLabels.push(" ! inconsistent_run_data" + elem["informationDate"] + "_v" + elem["reportVersion"]+ " datasetVersion: " + elem["datasetVersion"])
            continue
          }

        }

        let data = {
          labels: plotLabels,
          datasets: [
            {
              label: "Conformed",
              fill: true,
              lineTension: 0.1,
              backgroundColor: "green",
              borderColor: "green",
              borderCapStyle: 'butt',
              borderDash: [],
              borderDashOffset: 0.3,
              borderJoinStyle: 'miter',
              pointBorderColor: "rgba(75,192,192,1)",
              pointBackgroundColor: "#fff",
              pointBorderWidth: 1,
              pointHoverRadius: 5,
              pointHoverBackgroundColor: "rgba(75,192,192,1)",
              pointHoverBorderColor: "rgba(220,220,220,1)",
              pointHoverBorderWidth: 2,
              pointRadius: 1,
              pointHitRadius: 10,
              data: conformed,
              spanGaps: false,
              stack: "all"
            },
            {
              label: "Failed at conformance",
              fill: true,
              lineTension: 0.1,
              backgroundColor: "red",
              borderColor: "red",
              borderCapStyle: 'butt',
              borderDash: [],
              borderDashOffset: 0.0,
              borderJoinStyle: 'miter',
              pointBorderColor: "rgba(75,192,192,1)",
              pointBackgroundColor: "#fff",
              pointBorderWidth: 1,
              pointHoverRadius: 5,
              pointHoverBackgroundColor: "rgba(75,192,192,1)",
              pointHoverBorderColor: "rgba(220,220,220,1)",
              pointHoverBorderWidth: 2,
              pointRadius: 1,
              pointHitRadius: 10,
              data: failedAfterConformance,
              spanGaps: false,
              stack: "all"
            },
            {
              label: "Standartized, not conformed yet",
              fill: true,
              lineTension: 0.1,
              backgroundColor: "blue",
              borderColor: "blue",
              borderCapStyle: 'butt',
              borderDash: [],
              borderDashOffset: 0.0,
              borderJoinStyle: 'miter',
              pointBorderColor: "rgba(75,192,192,1)",
              pointBackgroundColor: "#fff",
              pointBorderWidth: 1,
              pointHoverRadius: 5,
              pointHoverBackgroundColor: "rgba(75,192,192,1)",
              pointHoverBorderColor: "rgba(220,220,220,1)",
              pointHoverBorderWidth: 2,
              pointRadius: 1,
              pointHitRadius: 10,
              data: waitingToConform,
              spanGaps: false,
              stack: "all"
            },

            {
              label: "Failed at standardization",
              fill: true,
              lineTension: 0.1,
              backgroundColor: "orange",
              borderColor: "orange",
              borderCapStyle: 'butt',
              borderDash: [],
              borderDashOffset: 0.0,
              borderJoinStyle: 'miter',
              pointBorderColor: "rgba(75,192,192,1)",
              pointBackgroundColor: "#fff",
              pointBorderWidth: 1,
              pointHoverRadius: 5,
              pointHoverBackgroundColor: "rgba(75,192,192,1)",
              pointHoverBorderColor: "rgba(220,220,220,1)",
              pointHoverBorderWidth: 2,
              pointRadius: 1,
              pointHitRadius: 10,
              data: failedAfterStandardization,
              spanGaps: false,
              stack: "all"
            },

            {
              label: "Unprocessed raw",
              fill: true,
              lineTension: 0.1,
              backgroundColor: "black",
              borderColor: "black",
              borderCapStyle: 'butt',
              borderDash: [],
              borderDashOffset: 0.0,
              borderJoinStyle: 'miter',
              pointBorderColor: "rgba(75,192,192,1)",
              pointBackgroundColor: "#fff",
              pointBorderWidth: 1,
              pointHoverRadius: 5,
              pointHoverBackgroundColor: "rgba(75,192,192,1)",
              pointHoverBorderColor: "rgba(220,220,220,1)",
              pointHoverBorderWidth: 2,
              pointRadius: 1,
              pointHitRadius: 10,
              data: unprocessedRaw,
              spanGaps: false,
              stack: "all"
            }
          ]
        };
        model.setProperty("/plotData", data)
      } else {
        // no datapoints for this interval
        model.setProperty("/plotData", [])
        model.setProperty("/monitoringPoints", [])
      }
    }, function() {
      sap.m.MessageBox
        .error("Failed to get monitoring points. Please wait a moment and try reloading the application")
    })
  }


  this.getDatasetList = function(bLoadFirst) {
    Functions.ajax("api/dataset/list", "GET", {}, function(oData) {
      model.setProperty("/datasets", oData)
    }, function() {
      sap.m.MessageBox
          .error("Failed to get the list of datasets. Please wait a moment and try reloading the application")
    })
  };

  this.setCurrentDataset = function(oDataset) {
    oDataset.conformance = oDataset.conformance.sort((first, second) => first.order > second.order);
    model.setProperty("/currentDataset", oDataset);
  };

  this.getDatasetCheckpoints = function(sId) {
    Functions.ajax("api/monitoring/checkpoints/datasets/" + encodeURI(sId), "GET", {}, function (oData) {
      model.setProperty("/checkpoints", oData)
    }, function () {
      sap.m.MessageBox
        .error("Failed to get checkpoints points. Please wait a moment and try reloading the application")
    })
  }

}
