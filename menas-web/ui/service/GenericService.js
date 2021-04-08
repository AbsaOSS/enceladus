/*
 * Copyright 2018 ABSA Group Limited
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

  const model = () => {
    return sap.ui.getCore().getModel()
  };

  let eventBus = sap.ui.getCore().getEventBus();
  const restClient = new RestClient();

  this.getUserInfo = function () {
    let fnSuccess = (oInfo) => {
      model().setProperty("/userInfo", oInfo);
      model().setProperty("/menasVersion", oInfo.menasVersion);
    };

    function getCookie(name) {
      let v = document.cookie.match('(^|;) ?' + name + '=([^;]*)(;|$)');
      return v ? v[2] : null;
    }

    let cookie = getCookie("JWT");
    let jwt = cookie ? cookie : localStorage.getItem("jwtToken");

    $.ajax(window.apiUrl + "/user/info", {
      headers: {
        "JWT": jwt
      },
      method: "GET",
      success: fnSuccess,
      async: false
    })
  };

  this.runStatusFormatter = function(sStatus) {
    switch(sStatus) {
      case "failed":                  return "Failed";
      case "successful":              return "Successful";
      case "successfulWithErrors":    return "Successful with errors";
      case "running":                 return "Running";
      case "stdSuccessful":           return "Standardization successful";
      default:                        return sStatus;
    }
  };

  this.runStatusColorFormatter = function(sStatus) {
    switch(sStatus) {
      case "failed":                  return "rgb(153, 0, 0)";
      case "successful":              return "rgb(0, 204, 0)";
      case "successfulWithErrors":    return "rgb(255, 255, 102)";
      case "running":                 return "rgb(153, 255, 153)";
      case "stdSuccessful":           return "rgb(255, 204, 102)";
      default:                        return "rgb(0,0,0)";
    }
  };

  this.getLandingPageInfo = function() {
    return RestClient.get("/landing/info").then((oData) => {
      model().setProperty("/landingPageInfo", oData);
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
      model().setProperty("/landingPageInfo/todayRunsGraph", graph);
      return oData;
    }).fail(() => {
      sap.m.MessageBox.error("Failed to load landing page information");
    })
  };

  this.getOozieInfo = function() {
    Functions.ajax("/oozie/isEnabled", "GET", {}, oData => {
      model().setProperty("/appInfo/oozie/isEnabled", oData);
    });
  };

  this.clearSession = function (sLogoutMessage) {
    model().setProperty("/userInfo", {});
    localStorage.clear();
    document.cookie = `JWT=; expires=Thu, 01 Jan 1970 00:00:01 GMT; path=${window.location.pathname.slice(0, -1)}`;
    eventBus.publish("nav", "logout");
    if (sLogoutMessage) {
      sap.m.MessageToast.show(sLogoutMessage, {
        duration: 15000
      })
    }
  };

  this.logout = function (sLogoutMessage) {
    this.clearSession(sLogoutMessage);
  };

  this.isEmpty = function (str) {
    return !str;
  };

  this.hasWhitespace = function (str) {
    return /\W/.test(str);
  };

  this.isValidColumnName = function (str) {
    return /^[a-zA-Z0-9._]+$/.test(str);
  };

  this.isValidFlatColumnName = function (str) {
    return /^[a-zA-Z0-9_]+$/.test(str);
  };

  this.isValidEntityName = function (sName) {
    return sName && sName !== "" && !this.hasWhitespace(sName);
  };

  this.isNameUnique = function(sName, oModel, sEntityType) {
    oModel.setProperty("/nameUsed", undefined);
    Functions.ajax("/" + sEntityType + "/isUniqueName/" + encodeURI(sName), "GET", {}, function(oData) {
      oModel.setProperty("/nameUnique", oData)
    }, function() {
      sap.m.MessageBox.error("Failed to retrieve isUniqueName. Please try again later.")
    })
  };

}();
