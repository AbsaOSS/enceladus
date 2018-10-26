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

sap.ui.controller("components.app", {

/**
* Called when a controller is instantiated and its View controls (if available) are already created.
* Can be used to modify the View before it is displayed, to bind event handlers and do other one-time initialization.
* @memberOf menas.main
*/
	onInit: function(oEv) {
		this.getView().setModel(sap.ui.getCore().getModel());
		this._router = sap.ui.core.UIComponent.getRouterFor(this)
		GenericService.getUserInfo();
		this._router.getRoute("root").attachMatched(function(oEvent) {
			this._router.navTo("schemas")
        }, this);
	},

	handleMenuPress: function(oEv) {
		this.byId("menasApp").hideMaster();
	},
	
	onSchemaPress: function(oEv) {
		this._router.navTo("schemas")
	},
	
	onDatasetPress: function(oEv) {
		this._router.navTo("datasets")
	},
	
	onMappingPress: function(oEv) {
		this._router.navTo("mappingTables")
	},
/**
* Similar to onAfterRendering, but this hook is invoked before the controller's View is re-rendered
* (NOT before the first rendering! onInit() is used for that one!).
* @memberOf menas.main
*/
//	onBeforeRendering: function() {
//
//	},

/**
* Called when the View has been rendered (so its HTML is part of the document). Post-rendering manipulations of the HTML could be done here.
* This hook is the same one that SAPUI5 controls get after being rendered.
* @memberOf menas.main
*/
	onAfterRendering: function() {
		component.setBusy(false)
	},

/**
* Called when the Controller is destroyed. Use this one to free resources and finalize activities.
* @memberOf menas.main
*/
//	onExit: function() {
//
//	}

});