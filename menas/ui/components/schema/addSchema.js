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

var AddSchemaFragment = function (oController, fnLoad) {

  let loadDialogFragment = () => {
    let oView = oController.getView();

    fnLoad({
      id: oView.getId(),
      name: "components.schema.addSchema",
      controller: oController
    }).then(function (oDialog) {
      oView.addDependent(oDialog);
    });

    return oController.byId("addSchemaDialog");
  };

  let oDialog = loadDialogFragment();

  let oFragment = {
    submit: function () {
      let newSchema = oController._model.getProperty("/newSchema");
      if (!newSchema.isEdit && newSchema.name && typeof (newSchema.nameUnique) === "undefined") {
        // need to wait for the service call
        setTimeout(this.submit.bind(this), 500);
        return;
      }

      if (this.isValid(newSchema)) {
        // send and update UI
        if (newSchema.isEdit) {
          let currSchema = oController._model.getProperty("/currentSchema");
          SchemaService.updateSchema(currSchema.name, currSchema.version, newSchema.description);
        } else {
          SchemaService.createSchema(newSchema.name, newSchema.description);
        }
        this.cancel(); // close & clean up
      }
    },

    cancel: function () {
      this.resetValueState();
      oController._model.setProperty("/newSchema", {});
      oDialog.close();
    },

    resetValueState: function () {
      oController.byId("newSchemaName").setValueState(sap.ui.core.ValueState.None);
      oController.byId("newSchemaName").setValueStateText("");
    },

    isValid: function (oSchema) {
      this.resetValueState();

      let hasValidName = EntityValidationService.hasValidName(oSchema, "Schema",
        oController.byId("newSchemaName"));

      return hasValidName;
    },

    onNameChange: function () {
      let sName = oController._model.getProperty("/newSchema/name");
      if (GenericService.isValidEntityName(sName)) {
        SchemaService.hasUniqueName(sName)
      } else {
        oController._model.setProperty("/newSchema/nameUnique", true)
      }
    }
  };

  this.getAdd = function () {
    oFragment.onPress = () => {
      oController._model.setProperty("/newSchema", {
        name: "",
        description: "",
        isEdit: false,
        title: "Add"
      });

      oDialog.open();
    };

    return oFragment;
  };

  this.getEdit = function () {
    oFragment.onPress = () => {
      let current = oController._model.getProperty("/currentSchema");
      current.isEdit = true;
      current.title = "Edit";

      oController._model.setProperty("/newSchema", jQuery.extend(true, {}, current));

      oDialog.open();
    };

    return oFragment;
  };

};
