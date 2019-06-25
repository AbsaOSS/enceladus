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

  this.getLandingPageInfo = function() {
    RestClient.get("api/landing/info").then((oData) => {
      model.setProperty("/landingPageInfo", oData);
    }).fail(() => {
      sap.m.MessageBox.error("Failed to load landing page information");
    })
  };

  this.getOozieInfo = function() {
    Functions.ajax("api/oozie/isEnabled", "GET", {}, oData => {
      model.setProperty("/appInfo/oozie/isEnabled", oData);
    });    
  };

  this.clearSession = function (sLogoutMessage) {
    model.setProperty("/userInfo", {});
    localStorage.clear();
    eventBus.publish("nav", "logout");
    if (sLogoutMessage) {
      sap.m.MessageToast.show(sLogoutMessage, {
        duration: 15000
      })
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
    Functions.ajax("api/" + sEntityType + "/isUniqueName/" + encodeURI(sName), "GET", {}, function(oData) {
      oModel.setProperty("/nameUnique", oData)
    }, function() {
      sap.m.MessageBox.error("Failed to retrieve isUniqueName. Please try again later.")
    })
  };

}();
