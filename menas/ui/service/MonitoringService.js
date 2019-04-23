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
    Functions.ajax("api/monitoring/" + encodeURI(sId), "GET", {}, function(oData) {
      model.setProperty("/monitoringPoints", oData)
      let plotLabels = []
      let raw_recordcount = []
      let std_records_succeeded = []
      let std_records_failed = []
      let conform_records_succeeded = []
      let conform_records_failed = []

      if (oData != undefined && oData.length != 0) {
        for (let elem of oData) {
          plotLabels.push(elem["informationDate"] + "_" + elem["reportVersion"])
          raw_recordcount.push(elem["raw_recordcount"])
          std_records_succeeded.push(elem["std_records_succeeded"])
          std_records_failed.push(elem["std_records_failed"])
          conform_records_succeeded.push(elem["conform_records_succeeded"])
          conform_records_failed.push(elem["conform_records_failed"])
        }
      }

      let data = {
        labels: plotLabels,
        datasets: [
          {
            label: "raw recordcount",
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
            data: raw_recordcount,
            spanGaps: false,
          },
          {
            label: "std_records_succeeded",
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
            data: std_records_succeeded,
            spanGaps: false,
            stack: "std"
          },
          {
            label: "std_records_failed",
            fill: true,
            lineTension: 0.1,
            backgroundColor: "firebrick",
            borderColor: "firebrick",
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
            data: std_records_failed,
            spanGaps: false,
            stack: "std"
          },
          {
            label: "conform_records_succeeded",
            fill: true,
            lineTension: 0.1,
            backgroundColor: "limegreen",
            borderColor: "limegreen",
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
            data: conform_records_succeeded,
            spanGaps: false,
            stack: "conform"
          },
          {
            label: "conform_records_failed",
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
            data: conform_records_failed,
            spanGaps: false,
            stack: "conform"
          }
        ]
      };
      model.setProperty("/plotData", data)
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

}();
