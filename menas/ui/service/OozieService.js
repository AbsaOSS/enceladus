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

var OozieService = new function () {

  const model = sap.ui.getCore().getModel();
  const eventBus = sap.ui.getCore().getEventBus();

  this.getCoordinatorStatus = function () {
    const coordinatorId = model.getProperty("/currentDataset/schedule/activeInstance/coordinatorId")
    if(coordinatorId) {
      Functions.ajax(`api/oozie/coordinatorStatus/${coordinatorId}`, "GET", {}, function (oData) {
        model.setProperty("/currentDataset/schedule/activeInstance/status", oData)
      }, function () {
      })      
    }
  };

}();
