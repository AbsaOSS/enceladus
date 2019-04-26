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

sap.ui.controller("components.monitoring.monitoringMain", {

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
      let arguments = oEvent.getParameter("arguments");
      this.routeMatched(arguments);
    }, this);

  },

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
      MonitoringService.getMonitoringPoints(oParams.id);
      MonitoringService.getDatasetCheckpoints(oParams.id);
      //this.preparePlotData(aMonitoringPoints)
      //this.getPlotData()
    }
  },

  /**
   * Called when the View has been rendered (so its HTML is part of the document).
   * Post-rendering manipulations of the HTML could be done here. This hook is the
   * same one that SAPUI5 controls get after being rendered.
   *
   * @memberOf components.dataset.datasetMain
   */

  /**
  preparePlotData: function (aMonitoringPoints) {
    let plotLabels = ["test"]
    let raw_recordcount = [1]
    //let std_records_succeeded = []
    //let std_records_failed = []
    //let conform_records_succeeded = []
    //let conform_records_failed = []
    //let testArray1 = []
    //let testArray2 = []
    let monitoringPoints = sap.ui.getCore().getModel().getProperty("/monitoringPoints")

    if (monitoringPoints != undefined && monitoringPoints.length != 0) {
      for (let elem of monitoringPoints) {
        plotLabels.push(elem["informationDate"] + "_" + elem["reportVersion"])
        raw_recordcount.push(elem["raw_recordcount"])
      }
    } else {
      alert(monitoringPoints)
    }

    let data = {
      labels: plotLabels,
      datasets: [
        {
          label: "raw recordcount",
          fill: false,
          lineTension: 0.1,
          backgroundColor: "rgba(75,192,192,0.4)",
          borderColor: "rgba(75,192,192,1)",
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
        }
      ]
    };

    model.setProperty("/plotData", data)
    //model.setProperty("/testArray1", testArray1)
    //model.setProperty("/testArray2", testArray2)
  },

  getPlotData: function () {
    let labels = ["January", "February", "March", "April", "May", "June", "July"]
    //labels.push("July")
    let data = {
      labels: labels,
      datasets: [
        {
          label: "My First dataset",
          fill: false,
          lineTension: 0.1,
          backgroundColor: "rgba(75,192,192,0.4)",
          borderColor: "rgba(75,192,192,1)",
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
          data: [65, 59, 80, 81, 56, 55, 40, 13],
          spanGaps: false,
        }
      ]
    };
    model.setProperty("/plotData", data)
  }
   **/
});
