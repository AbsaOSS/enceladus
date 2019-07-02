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

sap.ui.define(["sap/m/ListBase",
  "sap/m/Toolbar",
  "sap/m/Button",
  "sap/ui/core/Fragment",
  "sap/ui/model/Sorter",
  "sap/ui/model/json/JSONModel"],
    function(ListBase,
        Toolbar,
        Button,
        Fragment,
        Sorter,
        JSONModel) {

  const TableUtils = sap.ui.base.Object.extend("components.tables.TableUtils", {
    constructor: function(oControl, sTableName) {
      sap.ui.base.Object.prototype.constructor.apply(this, arguments);
      if(!oControl instanceof ListBase) {
        throw "oControl has to be an instance of sap.m.ListBase!"
      }
      this._oControl = oControl;
      this._sTableName = sTableName;
    }
  });

  TableUtils.prototype._addToolbar = function() {
    if(!this._oToolbar) {
      this._oToolbar = new Toolbar();
      if(this._sTableName) {
        this._oToolbar.addContent(new sap.m.Title({text: this._sTableName}));
      }
      this._oToolbar.addContent(new sap.m.ToolbarSpacer());
      this._oControl.setHeaderToolbar(this._oToolbar);
    }
  };

  TableUtils.prototype._sortDialogConfirm = function(oEv) {
    const mParams = oEv.getParameters();
    const oBinding = this._oControl.getBinding("items");

    const sPath = mParams.sortItem.getKey();
    const bDescending = mParams.sortDescending;
    const aSorters = [new Sorter(sPath, bDescending)];

    oBinding.sort(aSorters);
  };

  TableUtils.prototype._groupDialogConfirm = function(oEv) {
    const mParams = oEv.getParameters();
    const oBinding = this._oControl.getBinding("items");

    if(mParams.groupItem) {
      const sPath = mParams.groupItem.getKey();
      const bDescending = mParams.groupDescending;
      const aSorters = [new Sorter(sPath, bDescending, true)];

      oBinding.sort(aSorters);
    } else {
      oBinding.sort([]);
    }
  };

  TableUtils.prototype._getSearchFilter = function(sValue) {
    const filters = this._aSearchKeys.map((col) => {
      return new sap.ui.model.Filter(col, function(oAny) {
        let oVal;
        if(!oAny) {
          oVal = ""
        } else if(typeof(oAny) === "object") {
          oVal = JSON.stringify(oAny);
        } else if(typeof(oAny) === "string") {
          oVal = oAny;
        } else {
          oVal = oAny.toString();
        }
        return oVal.toUpperCase().indexOf(sValue.toUpperCase()) > -1;
      });
    });
    return new sap.ui.model.Filter(filters, false);
  };

  TableUtils.prototype._onSearch = function(oEv) {
    const sQuery = oEv.getSource().getValue();
    const oBinding = this._oControl.getBinding("items");
    if(sQuery) {
      const searchFilter = this._getSearchFilter(sQuery);
      oBinding.filter(searchFilter);
    } else {
      oBinding.filter([]);
    }
  };

  TableUtils.prototype._wrapCols = function(aCols, aKeys, sKeyIndex) {
    return aCols.map((col, ind) => {
      const res =  {
        "name": col
      };
      res[sKeyIndex] = aKeys[ind];
      return res;
    });
  };

  TableUtils.prototype._initSortDialog = function(aCols, aSortKeys) {
    Fragment.load({name: "components.tables.sortDialog"}).then((dialog) => {
      this._oSortDialog = dialog;
      const sortItems = this._wrapCols(aCols, aSortKeys, "sortKey")
      this._oSortDialog.setModel(new JSONModel({cols: sortItems}), "sort");
      this._oSortDialog.attachConfirm(this._sortDialogConfirm.bind(this));
      if(aSortKeys.length > 0) {
        this._oSortDialog.setSelectedSortItem(aSortKeys[0]);
      }
    });
  };

  TableUtils.prototype._initGroupDialog = function(aCols, aGroupKeys) {
    Fragment.load({name: "components.tables.groupDialog"}).then((dialog) => {
      this._oGroupDialog = dialog;
      const groupItems = this._wrapCols(aCols, aGroupKeys, "groupKey");
      this._oGroupDialog.setModel(new JSONModel({cols: groupItems}), "group");
      this._oGroupDialog.attachConfirm(this._groupDialogConfirm.bind(this));
    });
  };

  TableUtils.prototype._initSearchDialog = function() {
    Fragment.load({name: "components.tables.searchDialog"}).then((dialog) => {
      this._oSearchDialog = dialog;
      dialog.getContent()[0].attachSearch(this._onSearch.bind(this));
    });
  };

  TableUtils.prototype._addSort = function(aCols, aSortKeys) {
    this._oSortBtn = new Button({
      icon: "sap-icon://sort",
      press: () => {
        this._oSortDialog.open();
      }
    });
    this._oToolbar.addContent(this._oSortBtn);
    this._initSortDialog(aCols, aSortKeys);
  };

  TableUtils.prototype._addGroup = function(aCols, aGroupKeys) {
    this._oGroupBtn = new Button({
      icon: "sap-icon://group-2",
      press: () => {
        this._oGroupDialog.open();
      }
    });
    this._oToolbar.addContent(this._oGroupBtn);
    this._initGroupDialog(aCols, aGroupKeys);
  };

  TableUtils.prototype._addSearch = function(aSearchKeys) {
    this._oSearchBtn = new Button({
      icon: "sap-icon://search",
      press: () => {
        this._oSearchDialog.openBy(this._oSearchBtn);
      }
    });
    this._aSearchKeys = aSearchKeys;
    this._oToolbar.addContent(this._oSearchBtn);
    this._initSearchDialog();
  }

  TableUtils.prototype.makeSortable = function(aCols, aSortKeys) {
    this._addToolbar();
    if(!this._oControl._menasSortable) {
      this._addSort(aCols, aSortKeys);
    }
    this._oControl._menasSortable = true;
  };

  TableUtils.prototype.makeGroupable = function(aCols, aGroupKeys) {
    this._addToolbar();
    if(!this._oControl._menasGroupable) {
      this._addGroup(aCols, aGroupKeys);
    }
    this._oControl._menasGroupable = true;
  };

  TableUtils.prototype.makeSearchable = function(aSearchKeys) {
    this._addToolbar();
    if(!this._oControl._menasSearchable) {
      this._addSearch(aSearchKeys);
    }
    this._oControl._menasSearchable = true;
  };

  return TableUtils;
})
