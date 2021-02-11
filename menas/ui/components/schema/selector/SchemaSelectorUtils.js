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

sap.ui.define(["sap/ui/core/Fragment"], function(Fragment) {

  const SchemaSelectorUtils = sap.ui.base.Object.extend("components.schema.selector.SchemaSelectorUtils", { });

  SchemaSelectorUtils._initializeControl = function(oEv) {
    const oSrc = oEv.getSource();
    const oCtlId = oSrc.getId();
    const oModel = oSrc.getModel("schemas");
    const oEntModel = oSrc.getModel("entity");
    if(!oSrc._schemaSelectorDialog) {
      Fragment.load({
        name: "components.schema.selector.schemaSelectorDialog",
        id: oCtlId
      }).then((dialog) => {
        dialog.setModel(oModel, "schemas");
        dialog.setModel(oEntModel, "entity");
        oSrc._schemaSelectorDialog = dialog;
        dialog.open();
      })
    } else {
      oSrc._schemaSelectorDialog.setModel(oModel, "schemas");
      oSrc._schemaSelectorDialog.setModel(oEntModel, "entity");
      oSrc._schemaSelectorDialog.open();
    }
  }

  SchemaSelectorUtils.onOpenSelectDialog = function(oEv) {
    SchemaSelectorUtils._initializeControl(oEv);
  };

  SchemaSelectorUtils.onCancel = function(oEv) {
    oEv.getSource().getBinding("items").filter([]);
  };

  SchemaSelectorUtils.onSearch = function(oEv) {
    const sValue = oEv.getParameter("value");
    if(!sValue) {
      SchemaSelectorUtils.onCancel(oEv);
    } else {
      const oFilter = [new sap.ui.model.Filter("_id", function(sSchemaName) {
        return sSchemaName.toUpperCase().indexOf(sValue.toUpperCase()) > -1;
      })];
      oEv.getSource().getBinding("items").filter(oFilter);
    }
  };

  SchemaSelectorUtils.onAccept = function(oEv) {
    const oCtx = oEv.getParameter("selectedContexts")[0];
    const oDialog = oEv.getSource();
    const selectedSchema = oDialog.getModel("schemas").getProperty(oCtx.getPath());
    oDialog.getModel("entity").setProperty("/schemaName", selectedSchema._id);

    const eventBus = sap.ui.getCore().getEventBus();
    const schemaService = new SchemaService(oDialog.getModel("entity"), eventBus);
    schemaService.getAllVersions(selectedSchema._id, oDialog,
        oDialog.getModel("entity"), "/schemaVersion").then((oVersions) => {
          sap.ui.getCore().getModel().setProperty("/currentSchemaVersions", oVersions);
        });
  };

  return SchemaSelectorUtils;
})
