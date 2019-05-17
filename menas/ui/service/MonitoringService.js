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
      let inconsistentState = []

      let totalConformed = 0
      let totalFailedAfterConformance = 0
      let totalWaitingToConform = 0
      let totalFailedAfterStandardization = 0
      let totalUnprocessedRaw = 0
      let totalInconsistentState = 0

      let statusDict = {
        "allSucceeded" : 8,
        "stageSucceeded" : 5,
        "running" : 7,
        "failed" : 3
      }

      let statusPieData = {
        labels: [
          "allSucceeded",
          "stageSucceeded",
          "running",
          "failed",
          "other"
        ],
        datasets: [
          {
            data: [0,0,0,0,0],
            backgroundColor: [
              "green",
              "blue",
              "teal",
              "red",
              "magenta"
            ],
            hoverBackgroundColor: [
              "green",
              "blue",
              "teal",
              "red",
              "magenta"
            ]
          }]

      };


      if (oData != undefined && oData.length != 0) {

        for (let elem of oData) {
          let status = elem["status"]
          // check status
          switch (status) {
            case "allSucceeded":
              statusPieData["datasets"]["0"]["data"]["0"] += 1
              break
            case "stageSucceeded":
              statusPieData["datasets"]["0"]["data"]["1"] += 1
              break
            case "running":
              statusPieData["datasets"]["0"]["data"]["2"] += 1
              break
            case "failed":
              statusPieData["datasets"]["0"]["data"]["3"] += 1
              break
            default:
              statusPieData["datasets"]["0"]["data"]["4"] += 1
          }

          elem["infoLabel"] = statusDict[status]
          let rawTotal = elem["raw_recordcount"]
          let stdS = elem["std_records_succeeded"]
          let stdF = elem["std_records_failed"]
          let confS = elem["conform_records_succeeded"]
          let confF = elem["conform_records_failed"]
          plotLabels.push(elem["informationDate"] + " v" + elem["reportVersion"])

          // conformance finished
          if (rawTotal != undefined && stdS != undefined && stdF != undefined && confS != undefined && confF != undefined
            // sanity checks
            && (+stdS) + (+stdF) == (+rawTotal)
            && (+confS) + (+confF) == (+rawTotal)
            && (+stdF) <= (+confF)
            && (+stdS) >= (+confS)) {

              conformed.push(+confS)
              failedAfterConformance.push(confF - stdF)
              waitingToConform.push(0)
              failedAfterStandardization.push(+stdF)
              unprocessedRaw.push(0)
              inconsistentState.push(0)

              totalConformed += (+confS)
              totalFailedAfterConformance += confF - stdF
              totalFailedAfterStandardization += +stdF
              totalUnprocessedRaw += rawTotal - confS - confF

              continue

            // only standartization finished
          } else if (rawTotal != undefined && stdS != undefined && stdF != undefined && confS == undefined && confF == undefined
            //sanity checks
            && (+stdS) + (+stdF) == (+rawTotal) ) {

            conformed.push(0)
            failedAfterConformance.push(0)
            waitingToConform.push(+stdS)
            failedAfterStandardization.push(stdF)
            unprocessedRaw.push(0)
            inconsistentState.push(0)

            totalWaitingToConform += +stdS
            totalFailedAfterStandardization += +stdF
            totalUnprocessedRaw += rawTotal - stdS - stdF

            continue

            // only raw data is available
          } else if (rawTotal != undefined && stdS == undefined && stdF == undefined && confS == undefined && confF == undefined) {

            conformed.push(0)
            failedAfterConformance.push(0)
            waitingToConform.push(0)
            failedAfterStandardization.push(0)
            unprocessedRaw.push(+rawTotal)
            inconsistentState.push(0)

            totalUnprocessedRaw += +rawTotal

            continue

            // run data is inconsistent
          } else {
            conformed.push(0)
            failedAfterConformance.push(0)
            waitingToConform.push(0)
            failedAfterStandardization.push(0)
            unprocessedRaw.push(0)

            // try to estimate number of records involved in this run
            let estimatedRecordCount = 0
            if ( !isNaN(rawTotal) ) {
              estimatedRecordCount = +rawTotal
            } else if (!isNaN((+stdS) + (+stdF))){
              estimatedRecordCount = (+stdS) + (+stdF)
            } else if (!isNaN((+confS) + (+confF)) ) {
              estimatedRecordCount = (+confS) + (+confF)
            }
            inconsistentState.push(estimatedRecordCount)
            totalInconsistentState += estimatedRecordCount
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
            },
            {
              label: "Inconsistent run info",
              fill: true,
              lineTension: 0.1,
              backgroundColor: "magenta",
              borderColor: "magenta",
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
              data: inconsistentState,
              spanGaps: false,
              stack: "all"
            }
          ]
        };
        model.setProperty("/plotData", data)

        let recordCountTotals = {
          labels: [
            "Conformed",
            "Failed at conformance",
            "Standartized, not conformed yet",
            "Failed at standardization",
            "Unprocessed raw",
            "Inconsistent run info"
          ],
          datasets: [
            {
              data: [
                 totalConformed,
                 totalFailedAfterConformance,
                 totalWaitingToConform,
                 totalFailedAfterStandardization,
                 totalUnprocessedRaw,
                 totalInconsistentState
              ],
              backgroundColor: [
                "green",
                "red",
                "blue",
                "orange",
                "black",
                "magenta"
              ],
              hoverBackgroundColor: [
                "green",
                "red",
                "blue",
                "orange",
                "black",
                "magenta"
              ]
            }]

        };
        model.setProperty("/recordCountTotals", recordCountTotals)
        model.setProperty("/statusPieData", statusPieData)

      } else {
        // no datapoints for this interval
        model.setProperty("/plotData", [])
        //model.setProperty("/monitoringPoints", [])
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
