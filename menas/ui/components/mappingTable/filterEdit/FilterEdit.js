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


class FilterEdit {
  controlsInitd;

  /**
   *
   * @param idBase oController, or core
   * @param idPrefix prefix for ID, e.g. "MappingConformanceRule--". Default: empty string
   */
  constructor(idBase, idPrefix = "", schemaService) {
    this.idBase = idBase;
    this.idPrefix = idPrefix;
    this.schemaService = schemaService;

    this.controlsInitd = false;
  }

  getById(simpleId) {
    // in RL this can be e.g. controller.byId("someId") or sap.ui.getCore.byId("MappingConformanceRule--someId")
    return this.idBase.byId(this.idPrefix + simpleId);
  }

  bindFilterEditControls(oDialog) {
    if (!this.controlsInitd) { // prevent multiple controls attach

      this.dialog = oDialog;
      // filter toolbar:
      this.getById("addAndBtn").attachPress(this.onFilterAddAnd, this);
      this.getById("addOrBtn").attachPress(this.onFilterAddOr, this);
      this.getById("addNotBtn").attachPress(this.onFilterAddNot, this);
      this.getById("addEqualsBtn").attachPress(this.onFilterAddEquals, this);
      this.getById("addDiffersBtn").attachPress(this.onFilterAddDiffers, this);
      this.getById("addIsNullBtn").attachPress(this.onFilterAddIsNull, this);

      this.getById("removeSelectedBtn").attachPress(this.onRemoveSelected, this);

      this.controlsInitd = true;
    }
  }

  onFilterAddAnd() {
    this.onFilterAdd({_t: "AndJoinedFilters", filterItems: []})
  }

  onFilterAddOr() {
    this.onFilterAdd({_t: "OrJoinedFilters", filterItems: []})
  }

  onFilterAddNot() {
    this.onFilterAdd({_t: "NotFilter", inputFilter: null})
  }

  onFilterAddEquals() {
    this.onFilterAdd({_t: "EqualsFilter", columnName: "", value: ""})
  }

  onFilterAddDiffers() {
    this.onFilterAdd({_t: "DiffersFilter", columnName: "", value: ""})
  }

  onFilterAddIsNull() {
    this.onFilterAdd({_t: "IsNullFilter", columnName: ""})
  }

  onFilterAdd(blankFilter) {
    // blank filter contains validation fields:
    const namedBlankFilter = this.resetFilterDataValidation(FilterTreeUtils.addNiceNamesToFilterData(blankFilter));

    const treeTable = this.getById("filterTreeEdit");
    const selectedIndices = treeTable.getSelectedIndices();
    const treeTableModel = treeTable.getBinding().getModel();

    const currentFilters = this.dialog.getModel("filterEdit").getProperty("/editingFilters");
    const filtersEmpty = !currentFilters || currentFilters.filter(x => x).length == 0; // after removal of previous, there can be [null]

    if (filtersEmpty) {
      treeTableModel.setProperty("/editingFilters", [namedBlankFilter]); // add first filter by replacing the empty model

    } else if (selectedIndices.length == 1) {
      const newParentContext = treeTable.getContextByIndex(selectedIndices[0]);
      const newParent = newParentContext.getProperty();

      // based on what type of filter is selected, attach the new filter to it
      if (newParent._t == 'AndJoinedFilters' || newParent._t == 'OrJoinedFilters' ) { //and / or -> add
        newParent.filterItems = newParent.filterItems.concat(namedBlankFilter)
      } else if (newParent._t == 'NotFilter') {
        newParent.inputFilter = namedBlankFilter // not -> replace
      } else {
        sap.m.MessageToast.show("Could not add filter. Select AND, OR or NOT can have child filter added to. ");
        return;
      }
    } else {

      sap.m.MessageToast.show("Select exactly one item to add a child to!");
      return;
    }

    treeTableModel.refresh();
    if (selectedIndices) {
      treeTable.expand(selectedIndices[0]); // nice for the user to directly see the child among the expanded parent
    }
  }

  onRemoveSelected() {
    const treeTable = this.getById("filterTreeEdit");
    const selectedIndices = treeTable.getSelectedIndices();
    const treeTableModel = treeTable.getBinding().getModel();

    if (selectedIndices.length === 0) {
      sap.m.MessageToast.show("Select one or more items to remove.");
      return;
    }

    // delete the data
    selectedIndices.forEach(idx => {
        const context = treeTable.getContextByIndex(idx);
        const data = context.getProperty();

        if (data) {
          // The property is set to undefined to preserve the tree state (expand/collapse states of nodes).
          treeTableModel.setProperty(context.getPath(), undefined, context, true);
        }
      }
    );
  }

  /**
   * This method resets validations on the UI
   */
  resetFilterValidation() {
    const treeTable = this.getById("filterTreeEdit");
    const treeTableModel = treeTable.getBinding().getModel();

    const filterData = treeTableModel.getProperty("/editingFilters");

    // filter data can be [filter], [null] or null
    if (filterData && filterData.map(x => x).length != 0) {
      // resetting non-empty filter validations
      const resetValidatedFilter = this.resetFilterDataValidation(filterData[0]);
      treeTableModel.setProperty("/editingFilters", [resetValidatedFilter]);
    }
  }

  /**
   * This method operates on the data-object to immutably reset it (creates a copy with the reset validation fields)
   * @param filterData
   * @returns {copy} with reset validations
   */
  resetFilterDataValidation(filterData) {
    const resetFn = function (filterNode) {
      switch (filterNode._t) {
        case "AndJoinedFilters":
        case "OrJoinedFilters":
        case "NotFilter":
          filterNode.filter_valueState = "None";
          filterNode.filter_valueStateText = "";

          break;
        case "IsNullFilter":
          filterNode.filter_valueState = "None";
          filterNode.filter_valueStateText = "";

          filterNode.columnName_valueState = "None";
          filterNode.columnName_valueStateText = "";
          break;

        case "EqualsFilter":
        case "DiffersFilter":
          filterNode.filter_valueState = "None";
          filterNode.filter_valueStateText = "";

          filterNode.columnName_valueState = "None";
          filterNode.columnName_valueStateText = "";

          filterNode.value_valueState = "None";
          filterNode.value_valueStateText = "";
          break;
        default:
      }
    };

    return FilterTreeUtils.applyToFilterDataImmutably(filterData, resetFn);
  }

  /**
   * Validates data and emptiness in the filter TreeTable, sets their valueState|valueStateText (error+error descs)
   * @returns {empty: boolean, valid: boolean}
   */
  validateFilterData() {
    const treeTable = this.getById("filterTreeEdit");
    const treeTableModel = treeTable.getBinding().getModel();
    const filterData = treeTableModel.getProperty("/editingFilters");

    // filter data can be [filter], [null] or null
    if (!filterData || filterData.filter(x => x).length == 0) {
      return {
        empty: true,
        valid: true
      };
    }

    // nonempty filter: validate filter tree
    let filterValid = true;
    const validateInUiFn = function (filterNode) {
      switch (filterNode._t) {
        case "AndJoinedFilters":
        case "OrJoinedFilters":
          if (filterNode.filterItems.filter(x => x).length == 0) { // empty deleted ([null]) is not valid
            filterNode.filter_valueState = "Error";
            filterNode.filter_valueStateText = "Container filter must contain child filters!";
            filterValid = false;
          }
          break;

        case "NotFilter":
          if (!filterNode.inputFilter) {
            filterNode.filter_valueState = "Error";
            filterNode.filter_valueStateText = "Container filter must contain a child filter!";
            filterValid = false;
          }
          break;

        case "EqualsFilter":
        case "DiffersFilter":
          if (filterNode.columnName.length == 0) {
            filterNode.columnName_valueState = "Error";
            filterNode.columnName_valueStateText = "Select the column.";
            filterValid = false;
          }

          if (filterNode.value.length == 0) {
            filterNode.value_valueState = "Error";
            filterNode.value_valueStateText = "Fill in the value.";
            filterValid = false;
          }
          break;

        case "IsNullFilter":
          if (filterNode.columnName.length == 0) {
            filterNode.columnName_valueState = "Error";
            filterNode.columnName_valueStateText = "Fill in column name.";
            filterValid = false;
          }
          break;

        default:
      }
    };

    const validatedFilter = FilterTreeUtils.applyToFilterDataImmutably(filterData[0], validateInUiFn);
    treeTableModel.setProperty("/editingFilters", [validatedFilter]);
    treeTableModel.refresh();

    return {
      empty: false,
      valid: filterValid
    };
  }

  /**
   * Extract dot-separated schema names from schema **fields** tree with types (struct/arrays = treenodes), e.g.:
   * {{{
   *   [
   *     {name: root.subfield1.subsubfieldA, type: string},
   *     {name: root.subfield1.subsubfieldB, type: boolean},
   *     {name: root.subfieldB, type: integer},
   *   ]
   * }}}
   */
  static extractFieldNamesInDepth(schemaFields) {
    const extractedFields = schemaFields.map(field => {
      switch (field.type) {
        case "struct":
        case "array":
          const children = FilterEdit.extractFieldNamesInDepth(field.children);
          const prefix = field.name;

          // add prefix to all
          const prefixedChildren = children.map(child => {
            let childCopy = $.extend(true, {}, child); // being immutable
            childCopy.name = `${prefix}.${child.name}`; // prepending "parentName." for this recursion level
            return childCopy;
          });

          return prefixedChildren; // no break needed
        default:
          return [{name: field.name, type: field.type}]; // leaf field
      }
    });

    return extractedFields.flat(); // flat each recursive level
  }

  bindModelToSchemaChange(model) {
    const filterEdit = this;
    // setting binding from entity model to filterModel:
    // binding /selectedSchema (for both name and version) to avoid risking an inconsistent pair ("newSchema1", "versionOfThePreviousSchema")
    const binding = new sap.ui.model.Binding(model, "/selectedSchema", model.getContext("/"));
    binding.attachChange(function() {
      const selectedSchema = model.getProperty("/selectedSchema");

      if (selectedSchema) { // initially, on new MT, no schema is preselected in the dialog
        filterEdit.onUpdatedSchema(selectedSchema);
      }

    });
  }

  onUpdatedSchema(updatedSchema) {
    this.schemaService.getByNameAndVersion(updatedSchema.name, updatedSchema.version).then((schema) => {

      const allColumnNames = FilterEdit.extractFieldNamesInDepth(schema.fields);
      this.dialog.getModel("suggestedColumns").setProperty("/columnNames", allColumnNames);
    });

  }

  applyValueTypesFromSchema(filterData) {
    const allColsList = this.dialog.getModel("suggestedColumns").getProperty("/columnNames");
    const allColsKv = allColsList.map(x => [x.name, x.type]); // [{name1, type1}, ...] => [[name1, type1], ...]
    const allColsMap = new Map(allColsKv);

    const schemaFillFn = function (filterNode) {
      switch (filterNode._t) {
        case "EqualsFilter":
        case "DiffersFilter":
          const type = allColsMap.get(filterNode.columnName);
          if (type == undefined) {
            console.warn(`Could not determine type for column "${filterNode.columnName}"!`)
          } else {
            filterNode.valueType = type;
          }
          break;
        default:
      }
    };

    return FilterTreeUtils.applyToFilterDataImmutably(filterData, schemaFillFn);
  }
}
