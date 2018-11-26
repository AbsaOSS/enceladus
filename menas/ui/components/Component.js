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

sap.ui.define( ["sap/ui/core/UIComponent"], function (UIComponent) {
	"use strict";
	return UIComponent.extend("navigation", {

		metadata: {
			rootView: "components.app",
			routing: {
				config: {
					routerClass: "sap.m.routing.Router",
					viewPath: "",
					controlId: "menasApp",
					controlAggregation: "detailPages",
					viewType: "XML",
				},
				routes: [
					{
						name: "root",
						// empty hash - normally the start page
						pattern: "",
						target: ""
					},
					{
						name: "schemas",
						pattern: "schema/:id:/:version:", //here id and version are optional
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
					schemas: {
						viewName: "components.schema.schemaMain",
						viewLevel: 0,
						viewId: "schemaMainView"
					},
					dataset: {
						viewName: "components.dataset.datasetMain",
						viewLevel: 0,
						viewId: "datasetMainView"
					},
					mappingTable: {
						viewName: "components.mappingTable.mappingTableMain",
						viewLevel: 0,
						viewId: "mappingTableMainView"
					}
				}
			}
		},

		init : function () {
			UIComponent.prototype.init.apply(this, arguments);

			// Parse the current url and display the targets of the route that matches the hash
			this.getRouter().initialize();
		},
		busyIndicatorDelay: 0
	});
}, /* bExport= */ true);
