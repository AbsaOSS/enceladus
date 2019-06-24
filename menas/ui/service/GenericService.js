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
var GenericService = new function () {

  let model = sap.ui.getCore().getModel();

  let eventBus = sap.ui.getCore().getEventBus();
  const restClient = new RestClient();
  
  this.getUserInfo = function () {
    let fnSuccess = (oInfo) => {
      model.setProperty("/userInfo", oInfo)
    };

    $.ajax("api/user/info", {
      method: "GET",
      success: fnSuccess,
      async: false
    })
  };

  this.runStatusFormatter = function(sStatus) {
    if(sStatus === "failed") return "Failed";
    else if(sStatus === "successful") return "Successfull";
    else if(sStatus === "successfulWithErrors") return "Sucessful with errors";
    else if(sStatus === "running") return "Running";
    else if(sStatus === "stdSuccessful") return "Standardization successful";
    else sStatus;
  };

  this.runStatusColorFormatter = function(sStatus) {
    if(sStatus === "failed") return "rgb(153, 0, 0)";
    else if(sStatus === "successful") return "rgb(0, 204, 0)";
    else if(sStatus === "successfulWithErrors") return "rgb(255, 255, 102)";
    else if(sStatus === "running") return "rgb(153, 255, 153)";
    else if(sStatus === "stdSuccessful") return "rgb(255, 204, 102)";
    else "rgb(0,0,0)";
  };

  this.getLandingPageInfo = function() {
    RestClient.get("api/landing/info").then((oData) => {
      model.setProperty("/landingPageInfo", oData);
      const graphData = jQuery.extend({}, oData.todaysRunsStatistics);
      delete graphData["total"];
      const keys = Object.keys(graphData).map(this.runStatusFormatter);
      const vals = Object.values(graphData);
      const colors = Object.keys(graphData).map(this.runStatusColorFormatter);
      const graph = {
          "datasets": [{
            "data" : vals,
            "backgroundColor": colors
          }],
          "labels": keys
      };
      model.setProperty("/landingPageInfo/todayRunsGraph", graph);
    }).fail(() => {
      sap.m.MessageBox.error("Failed to load landing page information");
    })
  };

  this.getOozieInfo = function() {
    Functions.ajax("api/oozie/isEnabled", "GET", {}, oData => {
      model.setProperty("/appInfo/oozie/isEnabled", oData);
    });    
  }

  this.clearSession = function (sLogoutMessage) {
    model.setProperty("/userInfo", {});
    localStorage.clear();
    eventBus.publish("nav", "logout");
    if (sLogoutMessage) {
      sap.m.MessageToast.show(sLogoutMessage)
    }
  };

  this.logout = function (sLogoutMessage) {
    Functions.ajax("api/logout", "POST", {}, function () {
      // this is a dummy callback, returns 200 OK, but because ajax dataType is 'json' goes into error
    }, (xhr) => {
      if (xhr.status !== 403) {
        this.clearSession(sLogoutMessage);
      }
    })
  };

  this.hasWhitespace = function (str) {
    return /\W/.test(str);
  };

  this.isValidEntityName = function (sName) {
    return sName && sName !== "" && !this.hasWhitespace(sName);
  };

  this.isNameUnique = function(sName, oModel, sEntityType) {
    oModel.setProperty("/nameUsed", undefined);
    Functions.ajax("api/" + sEntityType + "/isUniqueName/" + encodeURI(sName), "GET", {}, function(oData) {
      oModel.setProperty("/nameUnique", oData)
    }, function() {
      sap.m.MessageBox.error("Failed to retrieve isUniqueName. Please try again later.")
    })
  };

}();
