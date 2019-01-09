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

sap.ui.controller("components.dataset.datasetMain", {

    /**
     * Called when a controller is instantiated and its View controls (if
     * available) are already created. Can be used to modify the View before it
     * is displayed, to bind event handlers and do other one-time
     * initialization.
     *
     * @memberOf components.dataset.datasetMain
     */
    onInit : function() {
        this._model = sap.ui.getCore().getModel();
        this._router = sap.ui.core.UIComponent.getRouterFor(this);
        this._router.getRoute("datasets").attachMatched(function(oEvent) {
            var arguments = oEvent.getParameter("arguments");
            this.routeMatched(arguments);
        }, this);

        this._addDialog = sap.ui.xmlfragment("components.dataset.addDataset", this);
        sap.ui.getCore().byId("newDatasetAddButton").attachPress(this.datasetAddSubmit, this);
        sap.ui.getCore().byId("newDatasetCancelButton").attachPress(this.datasetAddCancel, this);
        sap.ui.getCore().byId("newDatasetName").attachChange(this.datasetNameChange, this);
        sap.ui.getCore().byId("newDatasetSchemaNameSelect").attachChange(this.schemaSelect, this);
        this._addDialog.setBusyIndicatorDelay(0)
    },

    toSchema : function(oEv) {
        var src = oEv.getSource();
        sap.ui.core.UIComponent.getRouterFor(this).navTo("schemas", {
            id : src.data("name"),
            version : src.data("version")
        })
    },

    fetchSchema : function(oEv) {
        var dataset = sap.ui.getCore().getModel().getProperty("/currentDataset");
        if (typeof (dataset.schema) === "undefined") {
            SchemaService.getSchemaVersion(dataset.schemaName, dataset.schemaVersion, "/currentDataset/schema")
        }
    },

    datasetAddCancel : function() {
        var tree = sap.ui.getCore().byId("newDatasetRawHDFSBrowser");
        tree.unselectAll();
        tree.collapseAll();

        var treePublish = sap.ui.getCore().byId("newDatasetPublishDFSBrowser");
        treePublish.unselectAll();
        treePublish.collapseAll();

        this.resetNewDatasetValueState();
        this._addDialog.close();
    },

    datasetAddSubmit : function() {
        var newDataset = this._model.getProperty("/newDataset");
        // we may wanna wait for a call to determine whether this is unique
        if (!newDataset.isEdit && newDataset.name && typeof (newDataset.nameUnique) === "undefined") {
            // need to wait for the service call
            setTimeout(this.datasetAddSubmit.bind(this), 500);
            return;
        }
        if (this.validateNewDataset()) {
            // send and update UI
            if (newDataset.isEdit) {
                DatasetService.editDataset(newDataset.name, newDataset.version, newDataset.description, newDataset.hdfsPath, newDataset.hdfsPublishPath, newDataset.schemaName, newDataset.schemaVersion)
            } else {
                DatasetService.createDataset(newDataset.name, newDataset.description, newDataset.hdfsPath, newDataset.hdfsPublishPath, newDataset.schemaName, newDataset.schemaVersion)
            }
            this.datasetAddCancel(); // close & clean up
        }
    },

    resetNewDatasetValueState : function() {
        sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.None);
        sap.ui.getCore().byId("newDatasetName").setValueStateText("");

        sap.ui.getCore().byId("newDatasetSchemaNameSelect").setValueState(sap.ui.core.ValueState.None);
        sap.ui.getCore().byId("newDatasetSchemaNameSelect").setValueStateText("");

        sap.ui.getCore().byId("newDatasetSchemaVersionSelect").setValueState(sap.ui.core.ValueState.None);
        sap.ui.getCore().byId("newDatasetSchemaVersionSelect").setValueStateText("");

        sap.ui.getCore().byId("newDatasetRawHDFSBrowser").setValueState(sap.ui.core.ValueState.None);
        sap.ui.getCore().byId("newDatasetPublishDFSBrowser").setValueState(sap.ui.core.ValueState.None);
    },

    schemaSelect : function(oEv) {
        var sSchemaId = oEv.getParameter("selectedItem").getKey();
        SchemaService.getAllSchemaVersions(sSchemaId, sap.ui.getCore().byId("newDatasetSchemaVersionSelect"))
    },

    tabSelect : function(oEv) {
        if (oEv.getParameter("selectedKey") === "schema")
            this.fetchSchema();
    },

    _loadAllVersionsOfFirstSchema : function() {
        this._model.setProperty("/newDataset", {
            isEdit : false,
            title : "Add"
        });

        var schemas = this._model.getProperty("/schemas");

        if (schemas.length > 0) {
            this._model.setProperty("/newSchema", {
                schemaName : schemas[0]._id
            });

            var sSchema = this._model.getProperty("/schemas/0/_id");
            SchemaService.getAllSchemaVersions(sSchema, sap.ui.getCore().byId("newDatsetSchemaVersionSelect"))
        }
    },

    validateNewDataset : function() {
        this.resetNewDatasetValueState();
        var oDataset = this._model.getProperty("/newDataset");
        var isOk = true;

        if (!oDataset.name || oDataset.name === "") {
            sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.Error);
            sap.ui.getCore().byId("newDatasetName").setValueStateText("Dataset name cannot be empty");
            isOk = false;
        }
        if (!oDataset.isEdit && !oDataset.nameUnique) {
            sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.Error);
            sap.ui.getCore().byId("newDatasetName").setValueStateText(
                "Dataset name '" + oDataset.name + "' already exists. Choose a different name.");
            isOk = false;
        }
        if (GenericService.isValidEntityName(oDataset.name)) {
            sap.ui.getCore().byId("newDatasetName").setValueState(sap.ui.core.ValueState.Error);
            sap.ui.getCore().byId("newDatasetName").setValueStateText(
                "Dataset name '" + oDataset.name + "' should not have spaces. Please remove spaces and retry");
            isOk = false;
        }
        if (!oDataset.schemaName || oDataset.schemaName === "") {
            sap.ui.getCore().byId("newDatasetSchemaNameSelect").setValueState(sap.ui.core.ValueState.Error);
            sap.ui.getCore().byId("newDatasetSchemaNameSelect").setValueStateText("Please choose the schema of the dataset");
            isOk = false;
        }
        if (oDataset.schemaVersion === undefined || oDataset.schemaVersion === "") {
            sap.ui.getCore().byId("newDatasetSchemaVersionSelect").setValueState(sap.ui.core.ValueState.Error);
            sap.ui.getCore().byId("newDatasetSchemaVersionSelect").setValueStateText("Please choose the version of the schema for the dataset");
            isOk = false;
        }
        if (!oDataset.hdfsPath || oDataset.hdfsPath === "") {
            sap.ui.getCore().byId("newDatasetRawHDFSBrowser").setValueState(sap.ui.core.ValueState.Error);
            sap.m.MessageToast.show("Please choose the raw HDFS path of the dataset");
            isOk = false;
        }
        if (!oDataset.hdfsPublishPath || oDataset.hdfsPublishPath === "") {
            sap.ui.getCore().byId("newDatasetRawHDFSBrowser").setValueState(sap.ui.core.ValueState.Error);
            sap.m.MessageToast.show("Please choose the publish HDFS path of the dataset");
            isOk = false;
        }

        return isOk;
    },

    onAddPress : function() {
        this._loadAllVersionsOfFirstSchema();
        this._addDialog.open();
    },

    datasetNameChange : function() {
        DatasetService.isUniqueDatasetName(this._model.getProperty("/newDataset/name"))
    },

    onEditPress : function() {
        var current = this._model.getProperty("/currentDataset");

        current.isEdit = true;
        current.title = "Edit";

        this._model.setProperty("/newDataset", current);

        SchemaService.getAllSchemaVersions(current.schemaName, sap.ui.getCore().byId("newDatasetSchemaVersionSelect"));

        this._addDialog.open();
    },

    onRemovePress : function(oEv) {
        var current = this._model.getProperty("/currentDataset");

        sap.m.MessageBox.show("This action will remove all versions of the dataset definition. \nAre you sure?.", {
            icon : sap.m.MessageBox.Icon.WARNING,
            title : "Are you sure?",
            actions : [sap.m.MessageBox.Action.YES, sap.m.MessageBox.Action.NO],
            onClose : function(oAction) {
                if (oAction === "YES") {
                    DatasetService.disableDataset(current.name)
                }
            }
        });
    },

    datasetSelected : function(oEv) {
        var selected = oEv.getParameter("listItem").data("id");
        this._router.navTo("datasets", {
            id : selected
        });
    },

	routeMatched : function(oParams) {
		if (Prop.get(oParams, "id") === undefined) {
			DatasetService.getDatasetList(true);
		} else if (Prop.get(oParams, "version") === undefined) {
			DatasetService.getDatasetList();
			DatasetService.getLatestDatasetVersion(oParams.id)
		} else {
			DatasetService.getDatasetList();
			DatasetService.getDatasetVersion(oParams.id, oParams.version)
		}
	},

/**
 * Similar to onAfterRendering, but this hook is invoked before the controller's
 * View is re-rendered (NOT before the first rendering! onInit() is used for
 * that one!).
 *
 * @memberOf components.dataset.datasetMain
 */
// onBeforeRendering: function() {
//
// },
/**
 * Called when the View has been rendered (so its HTML is part of the document).
 * Post-rendering manipulations of the HTML could be done here. This hook is the
 * same one that SAPUI5 controls get after being rendered.
 *
 * @memberOf components.dataset.datasetMain
 */
    onAfterRendering : function() {
        // get schemas after rendering. This will be used for add/edit
        // functionality
        var schemas = this._model.getProperty("/schemas");
        if (!schemas || schemas.length === 0) {
            SchemaService.getSchemaList(false, true);
        }
    }
/**
 * Called when the Controller is destroyed. Use this one to free resources and
 * finalize activities.
 *
 * @memberOf components.dataset.datasetMain
 */
// onExit: function() {
//
// }
});
