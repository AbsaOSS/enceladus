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

  return Controller.extend("components.run.runDatasetVersionMaster", {

    onInit: function () {
      this._eventBus = sap.ui.getCore().getEventBus();
      this._eventBus.subscribe("runs", "list-grouped-version", this.list, this);

      this._router = sap.ui.core.UIComponent.getRouterFor(this);
    },

    list: function (channel, event, data) {
      let masterPage = this.byId("masterPage");
      masterPage.setModel(new sap.ui.model.json.JSONModel(data), "dataset");
      RunService.getRunsGroupedByDatasetVersion(masterPage, data.name);
    },

    onPressMasterBack: function () {
      this._eventBus.publish("nav", "back");
    },

    versionSelected: function (oEv) {
      let selectedItem = oEv.getParameter("listItem");
      let dataset = {
        name: selectedItem.data("name"),
        version: selectedItem.data("version")
      };

      this._eventBus.publish("runs", "dataset-version-selected", dataset);
      selectedItem.setSelected(false)
    },

  });
});
