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
  "sap/ui/core/mvc/Controller"
], function (Controller) {
  "use strict";

  return Controller.extend("components.run.runDatasetNameMaster", {

    onInit: function () {
      this._eventBus = sap.ui.getCore().getEventBus();
      this._eventBus.subscribe("runs", "list-grouped-name", this.list, this);

      this._router = sap.ui.core.UIComponent.getRouterFor(this);
    },

    list: function () {
      let masterPage = this.byId("masterPage");
      RunService.getRunsGroupedByDatasetName(masterPage);
    },

    onPressMasterBack: function () {
      this._eventBus.publish("nav", "back");
    },

    nameSelected: function (oEv) {
      let selectedItem = oEv.getParameter("listItem");
      let datasetName = selectedItem.data("name");

      this._eventBus.publish("runs", "dataset-name-selected", {name: datasetName});
      selectedItem.setSelected(false)
    },

  });
});
