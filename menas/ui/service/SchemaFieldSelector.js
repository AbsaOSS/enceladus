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

class SchemaFieldSelector {

  constructor(controller, dialog, outputPath) {
    this._controller = controller;
    this._dialog = dialog;
    this._outputPath = outputPath;
  }

  get controller() {
    return this._controller;
  }

  get dialog() {
    return this._dialog;
  }

  get outputPath() {
    return this._outputPath;
  }

  onSchemaFieldSelect(oEv) {
    let bindingPath = oEv.getParameter("listItem").getBindingContext("schema").getPath();
    let modelPathBase = "/fields/";
    let model = this.dialog.getModel("schema");
    this.controller._model.setProperty(this.outputPath, this._buildSchemaPath(bindingPath, modelPathBase, model));
  }

  _buildSchemaPath(bindingPath, modelPathBase, model) {
    let pathToks = bindingPath.replace(modelPathBase, "").split("/");

    let helper = (tokens, modelPathAccumulator, accumulator) => {
      if (tokens.length === 0) {
        return accumulator.join(".");
      }

      let rev = tokens.reverse();
      let sCurrPath = modelPathAccumulator + rev.pop() + "/";
      let curr = model.getProperty(sCurrPath);
      accumulator.push(curr.name);

      let newPath = sCurrPath + rev.pop() + "/";

      return helper(rev.reverse(), newPath, accumulator)
    };

    return helper(pathToks, modelPathBase, [])
  }

  preselectSchemaFieldSelector(sExpandTo, oControl, oScroll) {
    oControl.collapseAll();

    let aTokens = sExpandTo.split(".");
    let _preselectRadioButton = (aToks, oldSize, lastIndex) => {
      let newItems = oControl.getItems();
      let newSize = newItems.length - oldSize;
      let uniqueItems = newItems.slice(lastIndex+1, lastIndex+newSize+1);
      let itemToProcess = uniqueItems.find(item => this.getTitle(item) === aToks[0]);

      if(aToks.length === 1){
        itemToProcess.setSelected(true);
        let delegate = {
          onAfterRendering: function() {
            oScroll.scrollToElement(itemToProcess);
            oControl.removeEventDelegate(delegate);
          }.bind(this)
        };
        oControl.addEventDelegate(delegate);
      } else {
        let index = newItems.indexOf(itemToProcess);
        let oldSize = newItems.length;
        oControl.expand(index);
        _preselectRadioButton(aToks.slice(1), oldSize, index);
      }
    };

    _preselectRadioButton(aTokens, 0, -1);
  }

  getTitle(item) {
    const rawTitle = this.extractTitle(item);
    return this.parseTitle(rawTitle)
  }

  extractTitle(item) {
    const content = item.getAggregation("content")[0];
    const nestedItem = content.getItems()[0];
    return nestedItem.getProperty("htmlText");
  }

  parseTitle(rawTitle) {
    return rawTitle.match(/<strong>(?<title>.+)<\/strong>/).groups.title;
  }

}

class DefaultValueSchemaFieldSelector extends SchemaFieldSelector {

  constructor(controller, dialog) {
    super(controller, dialog, "/newDefaultValue/columnName");
  }

  preselectSchemaFieldSelector(sExpandTo, ruleType) {
    let oControl = sap.ui.getCore().byId("schemaFieldSelector");
    let oScroll = sap.ui.getCore().byId("fieldSelectScroll");
    super.preselectSchemaFieldSelector(sExpandTo, oControl, oScroll);
  }

}

class ConformanceRuleSchemaFieldSelector extends SchemaFieldSelector {

  constructor(controller, dialog) {
    super(controller, dialog, "/newRule/inputColumn");
  }

  preselectSchemaFieldSelector(sExpandTo, ruleType) {
    let oControl = sap.ui.getCore().byId(ruleType + "--schemaFieldSelector");
    let oScroll = sap.ui.getCore().byId(ruleType + "--fieldSelectScroll");
    super.preselectSchemaFieldSelector(sExpandTo, oControl, oScroll);
  }

}
