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

sap.ui.define([
  "sap/ui/core/mvc/Controller"
], function (Controller) {
  "use strict";

  return Controller.extend("components.run.runDetail", {

    /**
     * Called when a controller is instantiated and its View controls (if available) are already created.
     * Can be used to modify the View before it is displayed, to bind event handlers and do other one-time initialization.
     * @memberOf menas.main
     */
    onInit: function (oEv) {
      this._model = sap.ui.getCore().getModel();
      this._router = sap.ui.core.UIComponent.getRouterFor(this);
      this._router.getRoute("runs").attachMatched(function (oEvent) {
        let args = oEvent.getParameter("arguments");
        this.routeMatched(args);
      }, this);

      this._detail = this.byId("detailPage");
      this._checkpointsTable = this.byId("Checkpoints");
    },

    toDataset : function(oEv) {
      let src = oEv.getSource();
      sap.ui.core.UIComponent.getRouterFor(this).navTo("datasets", {
        id : src.data("name"),
        version : src.data("version")
      })
    },

    routeMatched: function (oParams) {
      if (Prop.get(oParams, "dataset") === undefined) {
        RunService.getFirstRun(this._detail, this._checkpointsTable);
      } else if (Prop.get(oParams, "version") === undefined) {
        RunService.getLatestRunForLatestVersion(this._detail, this._checkpointsTable, oParams.dataset)
      } else if (Prop.get(oParams, "id") === undefined) {
        RunService.getLatestRun(this._detail, this._checkpointsTable, oParams.dataset, oParams.version)
      } else {
        RunService.getRun(this._detail, this._checkpointsTable, oParams.dataset, oParams.version, oParams.id)
      }
    }

  });
});
