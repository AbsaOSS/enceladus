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
jQuery.sap.require("sap.m.MessageItem");
jQuery.sap.require("sap.m.MessagePopover");

sap.ui.controller("components.schema.schemaMain", {

	/**
	 * Called when a controller is instantiated and its View controls (if
	 * available) are already created. Can be used to modify the View before it
	 * is displayed, to bind event handlers and do other one-time
	 * initialization.
	 * 
	 * @memberOf components.schema.schemaMain
	 */
	onInit : function() {
		this._model = sap.ui.getCore().getModel()
		this._router = sap.ui.core.UIComponent.getRouterFor(this)
		this._router.getRoute("schemas").attachMatched(function(oEvent) {
			var arguments = oEvent.getParameter("arguments")
			this.routeMatched(arguments);
		}, this);

		// include CSRF to make spring security happy
		var token = $("meta[name='_csrf']").attr("content");
		var header = $("meta[name='_csrf_header']").attr("content");
		this.byId("fileUploader").addHeaderParameter(new sap.ui.unified.FileUploaderParameter({
			name : header,
			value : token
		}));

		SchemaService.getSchemaList();

		this._addDialog = sap.ui.xmlfragment("components.schema.addSchema", this);
		sap.ui.getCore().byId("newSchemaAddButton").attachPress(this.schemaAddSubmit, this)
		sap.ui.getCore().byId("newSchemaCancelButton").attachPress(this.schemaAddCancel, this)
		sap.ui.getCore().byId("newSchemaName").attachChange(this.schemaNameChange, this)

		this._editDialog = sap.ui.xmlfragment("components.schema.editSchema", this);
		sap.ui.getCore().byId("editSchemaSaveButton").attachPress(this.schemaEditSubmit, this)
		sap.ui.getCore().byId("editSchemaCancelButton").attachPress(this.schemaEditCancel, this)
		
	},

	onEditPress: function() {
		this._model.setProperty("/newSchema", this._model.getProperty("/currentSchema"));
		this._editDialog.open();
	},
	
	schemaNameChange : function() {
		SchemaService.isUniqueSchemaName(this._model.getProperty("/newSchema/name"))
	},

	usedInNavTo : function(oEv) {
		var source = oEv.getSource();
		sap.ui.core.UIComponent.getRouterFor(this).navTo(source.data("collection"), {
			id : source.data("name"),
			version : source.data("version")
		})
	},

	schemaAddSubmit : function() {
		var newSchema = this._model.getProperty("/newSchema")
		if (newSchema.name && typeof (newSchema.nameUnique) === "undefined") {
			// need to wait for the service call
			setTimeout(this.schemaAddSubmit.bind(this), 500);
		} else if (this.validateNewSchema()) {
			// send and update UI
			SchemaService.createSchema(newSchema.name, newSchema.description)
			this.schemaAddCancel(); // close & clean up
		}
	},

	schemaAddCancel : function() {
		this.resetNewSchemaValueState();
		this._model.setProperty("/newSchema", {});
		this._addDialog.close();
	},
	
	schemaEditCancel : function() {
		this._model.setProperty("/newSchema", {});
		this._editDialog.close();
	},
	
	schemaEditSubmit : function() {
		var newSchema = this._model.getProperty("/newSchema")
		var currSchema = this._model.getProperty("/currentSchema")
		SchemaService.updateSchema(currSchema.name, currSchema.version, newSchema.description)
		this.schemaEditCancel();
	},
	
	resetNewSchemaValueState : function() {
		sap.ui.getCore().byId("newSchemaName").setValueState(sap.ui.core.ValueState.None)
		sap.ui.getCore().byId("newSchemaName").setValueStateText("")
	},

	validateNewSchema : function() {
		this.resetNewSchemaValueState();
		var schema = this._model.getProperty("/newSchema")

		if (!schema.name || schema.name === "") {
			sap.ui.getCore().byId("newSchemaName").setValueState(sap.ui.core.ValueState.Error)
			sap.ui.getCore().byId("newSchemaName").setValueStateText("Schema name cannot be empty")
			return false;
		} else if (!schema.nameUnique) {
			sap.ui.getCore().byId("newSchemaName").setValueState(sap.ui.core.ValueState.Error)
			sap.ui.getCore().byId("newSchemaName").setValueStateText(
					"Schema name '" + schema.name + "' already exists. Choose a different name.")
			return false;
		} else
			return true;
	},


	onAddPress : function() {
		this._addDialog.open();
	},

	onPressLogout : function() {
		GenericService.logout();
	},

	schemaSelected : function(oEv) {
		var selected = oEv.getParameter("listItem").data("id")
		this._router.navTo("schemas", {
			id : selected
		});
	},
	
	metadataPress: function(oEv) {
		var binding = oEv.getSource().getBindingContext().getPath() + "/metadata";
		var bindingArr = binding + "Arr";
		//hmm bindAggregation doesn't take formatter :-/
		var arrMeta = Formatters.objToKVArray(this._model.getProperty(binding))
		this._model.setProperty(bindingArr, arrMeta)
		
		var oMessageTemplate = new sap.m.MessageItem({
			title: '{key}',
			subtitle: '{value}',
			type: sap.ui.core.MessageType.None
		});

		var oMessagePopover = new sap.m.MessagePopover({
			items: {
				path: bindingArr ,
				template: oMessageTemplate
			}
		}).setModel(this._model);
		
		oMessagePopover.toggle(oEv.getSource());
	},

	onRemovePress : function(oEv) {
		var current = this._model.getProperty("/currentSchema");

		sap.m.MessageBox.show("This action will remove all versions of the schema definition. \nAre you sure?.", {
			icon : sap.m.MessageBox.Icon.WARNING,
			title : "Are you sure?",
			actions : [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO],
			onClose : function(oAction) {
				if (oAction == "YES") {
					SchemaService.disableSchema(current.name)
				}
			}
		});
	},

	routeMatched : function(oParams) {
		if (Prop.get(oParams, "id") == undefined) {
			SchemaService.getSchemaList(true);
		} else if (Prop.get(oParams, "version") == undefined) {
			SchemaService.getLatestSchemaVersion(oParams.id)
		} else {
			SchemaService.getSchemaVersion(oParams.id, oParams.version)
		}
	},

	handleUploadPress : function(oParams) {
		sap.ui.core.BusyIndicator.show();
		this.byId("fileUploader").upload();
	},

	handleUploadProgress : function(oParams) {
		console.log((oParams.getParameter("loaded") / oParams.getParameter("total")) * 100)
	},

	handleUploadComplete : function(oParams) {
		sap.ui.core.BusyIndicator.hide();
		var status = oParams.getParameter("status")

		if (status === 500) {
			sap.m.MessageBox
					.error("Failed to upload new schema. Ensure that the file is a valid Spark-SQL JSON schema and try again.")
		} else if (status === 201) {
			sap.m.MessageToast.show("Schema successfully uploaded.")
			var oData = JSON.parse(oParams.getParameter("responseRaw"))
			model.setProperty("/currentSchema", oData)
			// update the list as well - maybe cheaper in future to update
			// locally
			SchemaService.getSchemaList();
			// nav back to info
			this.byId("schemaIconTabBar").setSelectedKey("info");
		}

	}

/**
 * Similar to onAfterRendering, but this hook is invoked before the controller's
 * View is re-rendered (NOT before the first rendering! onInit() is used for
 * that one!).
 * 
 * @memberOf components.schema.schemaMain
 */
// onBeforeRendering: function() {
//
// },
/**
 * Called when the View has been rendered (so its HTML is part of the document).
 * Post-rendering manipulations of the HTML could be done here. This hook is the
 * same one that SAPUI5 controls get after being rendered.
 * 
 * @memberOf components.schema.schemaMain
 */
// onAfterRendering: function() {
//
// },
/**
 * Called when the Controller is destroyed. Use this one to free resources and
 * finalize activities.
 * 
 * @memberOf components.schema.schemaMain
 */
// onExit: function() {
//
// }
});
