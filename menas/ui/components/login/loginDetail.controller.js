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

sap.ui.define([
  "sap/ui/core/mvc/Controller",
  "sap/m/MessageToast"
], function (Controller, MessageToast) {
  "use strict";

  const usernameField = "username";
  const passwordField = "password";

  function setCookie(name, value, days) {
    let d = new Date;
    d.setTime(d.getTime() + 24*60*60*1000*days);
    document.cookie = name + "=" + value + ";path=/;expires=" + d.toGMTString();
  }

  return Controller.extend("components.login.loginDetail", {
    loginForm: {},

    /**
     * Called when a controller is instantiated and its View controls (if
     * available) are already created. Can be used to modify the View before it
     * is displayed, to bind event handlers and do other one-time
     * initialization.
     *
     * @memberOf components.login.loginDetail
     */
    onInit: function () {
      this._eventBus = sap.ui.getCore().getEventBus();
      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("login").attachMatched(function (oEvent) {
        let config = oEvent.getParameter("config");
        this._appId = `${config.targetParent}--${config.controlId}`;
        this._appMasterId = `${config.targetParent}--${config.controlId}-Master`;
        this._appMasterBtnId = `${this._appMasterId}Btn`;
        this.handleMaster();
      }, this);

      ConfigRestClient.getEnvironmentName()
        .then( sEnvironmentName => {
          sap.ui.getCore().getModel().setProperty("/menasEnvironment", sEnvironmentName);
          document.title = `Menas ${sEnvironmentName}`;
        })
        .fail( () => console.log("Failed to get Environment name"));

      this._eventBus.subscribe("menas", "resize", this.handleMaster, this);
    },

    handleMaster: function() {
      const oCore = sap.ui.getCore();
      const oMaster = oCore.byId(this._appMasterId);
      const oMasterBtn = oCore.byId(this._appMasterBtnId);

      setTimeout(function() {
        if (typeof this._model.getProperty("/userInfo/username") === 'undefined') {
          if(oMaster) oMaster.setVisible(false);
          if(oMasterBtn) oMasterBtn.setVisible(false);
        } else {
          if(oMaster) oMaster.setVisible(true);
          if(oMasterBtn) oMasterBtn.setVisible(true);
        }
      }.bind(this), 50);

    },

    onAfterRendering: function() {
      if (sap.ui.Device.browser.chrome && sap.ui.Device.browser.version < 68) {
        sap.m.MessageBox.warning("Chrome browser version lower than 68 is known of having issue displaying the Menas application");
      } else if (sap.ui.Device.browser.firefox && sap.ui.Device.browser.version < 78) {
        sap.m.MessageBox.warning("Firefox browser version lower than 78 is known of having issue displaying the Menas application");
      } else if (sap.ui.Device.browser.edge && sap.ui.Device.browser.version < 79) {
        sap.m.MessageBox.warning("Edge browser version lower than 79 is known of having issue displaying the Menas application");
      } else if (sap.ui.Device.browser.safari && sap.ui.Device.browser.version < 12) {
        sap.m.MessageBox.warning("Safari browser version lower than 12 is known of having issue displaying the Menas application");
      } else if (!sap.ui.Device.browser.chrome && !sap.ui.Device.browser.firefox && !sap.ui.Device.browser.edge && !sap.ui.Device.browser.safari) {
        sap.m.MessageBox.warning("Your browser is not known if it to works wih Menas application");
      }
    },

    onLoginSubmit: function (oEvent) {
      let oData = {
          username: this.byId(usernameField).getValue(),
          password: this.byId(passwordField).getValue()
      };

      this._resetLoginFormState();
      if (this._validateLogin(oData)) {
        this._login(oData, this.byId("loginSubmit"))
      }
    },

    _resetFieldState: function (sField) {
      this.byId(sField).setValueState(sap.ui.core.ValueState.None);
      this.byId(sField).setValueStateText("");
    },

    _resetLoginFormState: function () {
      this._resetFieldState(usernameField);
      this._resetFieldState(passwordField);
    },

    _validateField: function (oData, sField, sErrorMessage) {
      let isOk = oData[sField] && oData[sField] !== "";

      if (!isOk) {
        let field = this.byId(sField);
        field.setValueState(sap.ui.core.ValueState.Error);
        field.setValueStateText(sErrorMessage);
      }

      return isOk;
    },

    _validateLogin(oData) {
      let isValidUsername = this._validateField(oData, usernameField, "Username cannot be empty.");
      let isValidPassword = this._validateField(oData, passwordField, "Password cannot be empty.");
      return isValidUsername && isValidPassword;
    },

    _login: function (oData, oControl) {
      if (oControl) oControl.setBusy(true);

      let fnSuccess = (result, status, xhr) => {
        this.byId("password").setValue("");
        let csrfToken = xhr.getResponseHeader("X-CSRF-TOKEN");
        let jwt = xhr.getResponseHeader("JWT");
        localStorage.setItem("csrfToken", csrfToken);
        localStorage.setItem("jwtToken", jwt);
        setCookie("JWT", jwt, 1);
        Functions.ajax("/user/info", "GET", {}, (oInfo) => {
          model.setProperty("/userInfo", oInfo);
          model.setProperty("/menasVersion", oInfo.menasVersion);
          sap.ui.getCore().byId(this._appId).backToTopMaster();
          this.handleMaster();
          this._router.navTo("home");
          this._eventBus.publish("nav", "login");
        });
        PropertiesService.getProperties(); // refresh properties when logged in
      };

      let fnError = () => {
        MessageToast.show("Username or password incorrect");
        this.byId(usernameField).setValueState(sap.ui.core.ValueState.Error);
        this.byId(passwordField).setValueState(sap.ui.core.ValueState.Error);
      };
      $.ajax(window.apiUrl + "/login", {
        complete: function () {
          if (oControl) oControl.setBusy(false)
        },
        data: oData,
        method: "POST",
        success: fnSuccess,
        error: fnError
      })
    },

  });

});
