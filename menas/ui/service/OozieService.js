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

jQuery.sap.require("sap.m.MessageToast");
jQuery.sap.require("sap.m.MessageBox");

var OozieService = new function () {

  const model = () => {
    return sap.ui.getCore().getModel();
  }
  const eventBus = sap.ui.getCore().getEventBus();

  this.getCoordinatorStatus = function () {
    const coordinatorId = model().getProperty("/currentDataset/schedule/activeInstance/coordinatorId")
    if(coordinatorId) {
      RestClient.get(`api/oozie/coordinatorStatus/${coordinatorId}`).then((oData) => {
        model().setProperty("/currentDataset/schedule/activeInstance/status", oData);
      })
    }
  };

  this.runNow = function(oCtl) {
    const oSchedule = model().getProperty("/currentDataset/schedule")
    if(oSchedule) {
      RestClient.post("api/oozie/runNow", oSchedule, oCtl)
        .then((oData) => {
          sap.m.MessageToast.show(`The job has been submitted with ID: ${oData}`, {duration: 10000});
        })
        .fail((err) => {
          const error = JSON.parse(err.responseText);
          sap.m.MessageBox.error(`${error.message}\nError id: ${error.id}`);
        });
    }
  };

  this.suspend = function(oCtl) {
    const oSchedule = model().getProperty("/currentDataset/schedule")
    if(oSchedule && oSchedule.activeInstance) {
      RestClient.post(`api/oozie/suspend/${oSchedule.activeInstance.coordinatorId}`, undefined, oCtl).then((oData) => {
        if(oData.status === "SUSPENDED") {
          sap.m.MessageToast.show(`Schedule ${oSchedule.activeInstance.coordinatorId} has been suspended`, {duration: 10000});
        } else {
          sap.m.MessageToast.show(`Failed to suspend schedule: ${oSchedule.activeInstance.coordinatorId}!`, {duration: 10000});
        }
        model().setProperty("/currentDataset/schedule/activeInstance/status", oData);
      })
    }
  };

  this.resume = function(oCtl) {
    const oSchedule = model().getProperty("/currentDataset/schedule")
    if(oSchedule && oSchedule.activeInstance) {
      RestClient.post(`api/oozie/resume/${oSchedule.activeInstance.coordinatorId}`, undefined, oCtl).then((oData) => {
        if(oData.status === "RUNNING") {
          sap.m.MessageToast.show(`Schedule ${oSchedule.activeInstance.coordinatorId} has been resumed`, {duration: 10000});
        } else {
          sap.m.MessageToast.show(`Failed to resume schedule: ${oSchedule.activeInstance.coordinatorId}!`, {duration: 10000});
        }
        
        model().setProperty("/currentDataset/schedule/activeInstance/status", oData);
      })
    }
  };

}();
