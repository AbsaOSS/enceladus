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
    "sap/ui/core/UIComponent"
  ],
  function (UIComponent) {
    "use strict";

    return UIComponent.extend("navigation", {
      metadata: {
        rootView : {
          viewName: "components.app",
          id: "rootView", 
          type: "XML" 
        },
        routing: {
          config: {
            routerClass: "sap.m.routing.Router",
            viewPath: "",
            controlId: "menasApp",
            controlAggregation: "detailPages",
            viewType: "XML"
          },
          routes: [
            {
              name: "root",
              // empty hash - normally the start page
              pattern: "",
              target: ""
            },
            {
              name: "login",
              pattern: "login",
              target: "login"
            },
            {
              name: "runs",
              pattern: "runs/:dataset:/:version:/:id:",
              target: "runs"
            },
            {
              name: "schemas",
              pattern: "schema/:id:/:version:", // here id and version are optional
              target: "schemas"
            },
            {
              name: "datasets",
              pattern: "dataset/:id:/:version:",
              target: "dataset"
            },
            {
              name: "mappingTables",
              pattern: "mapping/:id:/:version:",
              target: "mappingTable"
            }
          ],
          targets: {
            login: {
              viewName: "components.login.loginMain",
              viewLevel: 0,
              viewId: "loginMainView"
            },
            runs: {
              viewName: "components.run.runMain",
              viewLevel: 1,
              viewId: "runMainView"
            },
            schemas: {
              viewName: "components.schema.schemaMain",
              viewLevel: 1,
              viewId: "schemaMainView"
            },
            dataset: {
              viewName: "components.dataset.datasetMain",
              viewLevel: 1,
              viewId: "datasetMainView"
            },
            mappingTable: {
              viewName: "components.mappingTable.mappingTableMain",
              viewLevel: 1,
              viewId: "mappingTableMainView"
            }
          }
        }
      },

      init: function () {
        UIComponent.prototype.init.apply(this, arguments);

        // Parse the current url and display the targets of the route that matches the hash
        this.getRouter().initialize();
      },
      busyIndicatorDelay: 0
    });
  }, /* bExport= */ true);
