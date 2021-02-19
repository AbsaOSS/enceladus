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
  "./../external/it/designfuture/chartjs/library-preload",
  "sap/ui/core/format/NumberFormat"
  ], function (Controller, Openui5Chartjs, NumberFormat) {
  "use strict";

  return Controller.extend("components.home.landingPage", {

    onInit: function() {
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("home").attachMatched(function(oEvent) {
        const config = oEvent.getParameter("config");
        this._appId = `${config.targetParent}--${config.controlId}`;
        this._app = sap.ui.getCore().byId(this._appId);
        const oPage = this.byId("landingPage");

        oPage.setBusyIndicatorDelay(0);
        oPage.setBusy(true);
        GenericService.getLandingPageInfo().always(() => {
          oPage.setBusy(false);
        });
      }, this);
      this._eventBus = sap.ui.getCore().getEventBus();
      this._tileNumFormat = NumberFormat.getIntegerInstance({
        shortLimit: 9999,
        style: "short"
      });

      ConfigRestClient.getEnvironmentName()
        .then( sEnvironmentName => {
          sap.ui.getCore().getModel().setProperty("/menasEnvironment", sEnvironmentName);
          document.title = `Menas ${sEnvironmentName}`;
        })
        .fail( () => console.log("Failed to get Environment name"));
    },

    tileNumberFormatter: function(nNum) {
      return this._tileNumFormat.format(nNum);
    },

    reShowMaster: function() {
      if(this._app._bMasterClosing) {
        setTimeout(function() {
          this.reShowMaster()
        }.bind(this), 100);
      } else {
        this._app.showMaster();
      }
    },

    masterNavigate: function(oEv) {
      const oSrc = oEv.getSource();
      const sTarget = oSrc.data("target");

      let viewBase = `__navigation0---rootView`;

      if(sTarget === "datasets") {
        viewBase = `${viewBase}--datasetsPage`;
      } else if(sTarget === "schemas") {
        viewBase = `${viewBase}--schemasPage`;
      } else if(sTarget === "mappingTables") {
        viewBase = `${viewBase}--mappingTablesPage`;
      } else if(sTarget === "runs") {
        viewBase = `${viewBase}--runsDatasetNamePage`;
      }

      if (sTarget === "runs") {
        this._eventBus.publish(sTarget, "list-grouped-name");
      } else {
        this._eventBus.publish(sTarget, "list");
      }

      this._app.backToTopMaster();
      this._app.toMaster(viewBase);

      this._app.showMaster();
      //it will auto-close when the window size is small enough
      setTimeout(function() {
        this.reShowMaster();
      }.bind(this), 100);

    }

  })
});
