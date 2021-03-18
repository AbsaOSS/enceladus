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

class ConformanceRuleForm {

  constructor(ruleType, schemaFieldSelectorSupportedRule) {
    if (this.isCorrectlyConfigured === undefined) {
      throw new TypeError("Abstract function 'isCorrectlyConfigured' not implemented.");
    }

    this._ruleType = ruleType;
    this._schemaFieldSelectorSupportedRule = schemaFieldSelectorSupportedRule;
  }

  get ruleType() {
    return this._ruleType;
  }

  get hasSchemaFieldSelector() {
    return this._schemaFieldSelectorSupportedRule;
  }

  get outputColumnControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--outputColumn`);
  }

  get inputColumnControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--schemaFieldSelector`);
  }

  isValid(rule, schemas, rules) {
    let hasValidOutput = true;
    if (rule._t !== "MappingConformanceRule") {
      hasValidOutput = this.hasValidOutputColumn(rule.outputColumn, schemas[rule.order]);
    }

    const hasValidOutputColumn = hasValidOutput && this.hasValidTransitiveSchema(rule, schemas, rules);
    return hasValidOutputColumn & this.isCorrectlyConfigured(rule);
  }

  hasValidOutputColumn(fieldValue, schema) {
    let isValid = this.hasValidColumn(fieldValue, "Output Column", this.outputColumnControl);

    if (isValid && fieldValue.includes(".")) {
      const isPathToStruct = SchemaManager.validatePathOfStructs(fieldValue, schema.fields);
      if (!isPathToStruct.isValid) {
        this.outputColumnControl.setValueState(sap.ui.core.ValueState.Error);
        this.outputColumnControl.setValueStateText(isPathToStruct.error);
      }

      isValid = isPathToStruct.isValid;
    }

    return isValid;
  }

  hasValidColumn(fieldValue, fieldName, inputControl) {
    let isValid = this.nonEmptyField(fieldValue, fieldName, inputControl);

    if (isValid && !GenericService.isValidColumnName(fieldValue)) {
      inputControl.setValueState(sap.ui.core.ValueState.Error);
      inputControl.setValueStateText(`${fieldName} can only contain alphanumeric characters, underscores and dots`);
      isValid = false;
    }

    return isValid;
  }

  hasValidFlatColumn(fieldValue, fieldName, inputControl) {
    let isValid = this.nonEmptyField(fieldValue, fieldName, inputControl);

    if (isValid && !GenericService.isValidFlatColumnName(fieldValue)) {
      inputControl.setValueState(sap.ui.core.ValueState.Error);
      inputControl.setValueStateText(`${fieldName} can only contain alphanumeric characters and underscores`);
      isValid = false;
    }

    return isValid;
  }

  nonEmptyField(fieldValue, fieldName, inputControl) {
    let isValid = true;

    if (GenericService.isEmpty(fieldValue)) {
      inputControl.setValueState(sap.ui.core.ValueState.Error);
      inputControl.setValueStateText(`${fieldName} cannot be empty`);
      isValid = false;
    }

    return isValid
  }

  hasValidInputColumn(fieldValue) {
    return this.hasValidSchemaSelection(fieldValue, this.inputColumnControl);
  }

  hasValidSchemaSelection(fieldValue, inputControl) {
    let isValid = true;

    if (GenericService.isEmpty(fieldValue) || !GenericService.isValidColumnName(fieldValue)) {
      inputControl
        .getItems()
        .forEach(item => item.setHighlight(sap.ui.core.ValueState.Error));
      isValid = false;
    }

    return isValid;
  }

  hasValidTransitiveSchema(rule, schemas, rules) {
    const validation = SchemaManager.validateNameClashes(rule, schemas, rules);

    if (!validation.isValid) {
      if (this.outputColumnControl){
        this.outputColumnControl.setValueState(sap.ui.core.ValueState.Error);
        this.outputColumnControl.setValueStateText(validation.error);
      } else {
        sap.m.MessageToast.show(validation.error);
      }
    }

    return validation.isValid;
  }

  reset() {
    this.resetOutputColumn();
  }

  resetOutputColumn() {
    this.resetValueState(this.outputColumnControl)
  }

  resetInputColumn() {
    this.inputColumnControl
      .getItems()
      .forEach(item => item.setHighlight(sap.ui.core.ValueState.None));
  }

  resetValueState(inputControl) {
    inputControl.setValueState(sap.ui.core.ValueState.None);
    inputControl.setValueStateText("");
  }

}

class CastingConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("CastingConformanceRule", true)
  }

  get dataTypes() {
    return [
      {type: "boolean"},
      {type: "byte"},
      {type: "short"},
      {type: "integer"},
      {type: "long"},
      {type: "float"},
      {type: "double"},
      {type: "decimal(38,18)"},
      {type: "string"},
      {type: "date"},
      {type: "timestamp"},
      {type: "binary"}
    ]
  }

  get outputDataTypeControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--outputDataType`);
  }

  isCorrectlyConfigured(rule) {
    return this.hasValidInputColumn(rule.inputColumn)
      & this.hasValidOutputDataType(rule.outputDataType);
  }

  hasValidOutputDataType(fieldValue) {
    const inputControl = this.outputDataTypeControl;
    let isValid = true;

    if (GenericService.isEmpty(fieldValue)) {
      inputControl.setValueState(sap.ui.core.ValueState.Error);
      inputControl.setValueStateText("Output Data Type cannot be empty");
      isValid = false;
    } else if (!this.dataTypes.some(dt => dt.type === fieldValue)) {
      inputControl.setValueState(sap.ui.core.ValueState.Error);
      inputControl.setValueStateText("Output Data Type is unrecognized");
      isValid = false;
    }

    return isValid;
  }

  reset() {
    super.reset();
    this.resetInputColumn();
    this.resetValueState(this.outputDataTypeControl);
  }

}

class ConcatenationConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("ConcatenationConformanceRule", false)
  }

  get inputColumnsControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--inputColumns`);
  }

  isCorrectlyConfigured(rule) {
    return this.hasValidInputColumns(rule.inputColumns);
  }

  hasValidInputColumns(fieldValue = []) {
    let isValid = fieldValue.length >= 2;

    if (!isValid) {
      this.inputColumnsControl.getItems().forEach(item => item.setHighlight(sap.ui.core.ValueState.Error));
      sap.m.MessageToast.show("At least 2 columns to concatenate are required.");
    }

    return isValid
  }

  reset() {
    super.reset();
    this.resetInputColumns();
  }

  resetInputColumns() {
    this.inputColumnsControl
      .getItems()
      .forEach(item => item.setHighlight(sap.ui.core.ValueState.None));
  }

}

class DropConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("DropConformanceRule", true)
  }

  get outputColumnControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--schemaFieldSelector`);
  }

  isCorrectlyConfigured(rule) {
    return true;
  }

  hasValidOutputColumn(fieldValue) {
    return this.hasValidSchemaSelection(fieldValue, this.outputColumnControl);
  }

  reset() {
    this.outputColumnControl
      .getItems()
      .forEach(item => item.setHighlight(sap.ui.core.ValueState.None));
  }

  hasValidTransitiveSchema(rule, schemas, rules) {
    const validation = SchemaManager.validateColumnRemoval(rule, schemas, rules);

    if (!validation.isValid) {
      this.outputColumnControl
        .getItems()
        .forEach(item => item.setHighlight(sap.ui.core.ValueState.None));
      sap.m.MessageToast.show(validation.error);
    }

    return validation.isValid;
  }

}

class LiteralConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("LiteralConformanceRule", false)
  }

  get literalValueControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--literalValue`);
  }

  isCorrectlyConfigured(rule) {
    return this.hasValidLiteralValue(rule.value);
  }

  hasValidLiteralValue(fieldValue) {
    return this.nonEmptyField(fieldValue, "Literal Value", this.literalValueControl);
  }

  reset() {
    super.reset();
    this.resetValueState(this.literalValueControl);
  }

}

class FillNullsConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("FillNullsConformanceRule", true)
  }

  get fillNullsValueControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--fillNullsValue`);
  }

  isCorrectlyConfigured(rule) {
    return this.nonEmptyField(rule.value, "Fill Nulls with Value", this.fillNullsValueControl);
  }

  reset() {
    super.reset();
    this.resetValueState(this.fillNullsValueControl);
  }

}

class CoalesceConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("CoalesceConformanceRule", false)
  }

  get inputColumnsControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--inputColumns`);
  }

  isCorrectlyConfigured(rule) {
    return this.hasValidInputColumns(rule.inputColumns);
  }

  hasValidInputColumns(fieldValue = []) {
    let isValid = fieldValue.length >= 2;

    if (!isValid) {
      this.inputColumnsControl.getItems().forEach(item => item.setHighlight(sap.ui.core.ValueState.Error));
      sap.m.MessageToast.show("At least 2 columns are required for coalesce.");
    }

    return isValid
  }

  reset() {
    super.reset();
    this.resetInputColumns();
  }

  resetInputColumns() {
    this.inputColumnsControl
      .getItems()
      .forEach(item => item.setHighlight(sap.ui.core.ValueState.None));
  }

}

class MappingConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("MappingConformanceRule", true)
  }

  isCorrectlyConfigured(rule) {
    return this.hasValidInputColumn(rule.targetAttribute)
      & this.hasValidOutputColumns(rule)
      & this.hasValidJoinConditions(rule.newJoinConditions);
  }

  hasValidJoinConditions(fieldValue = []) {
    let isValid = fieldValue.length >= 1;

    if (!isValid) {
      sap.m.MessageToast.show("At least 1 join condition is required.");
    }

    return isValid
  }

  hasValidOutputColumns(rule = []) {
    let isValid = rule.additionalColumns.length >= 1 || rule.outputColumn !== undefined;

    if (!isValid) {
      sap.m.MessageToast.show("At least 1 output column is required.");
    }

    return isValid
  }

  reset() {
    // super.reset();
  }

}

class NegationConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("NegationConformanceRule", true)
  }

  isCorrectlyConfigured(rule) {
    return this.hasValidInputColumn(rule.inputColumn);
  }

  reset() {
    super.reset();
    this.resetInputColumn();
  }

}

class SingleColumnConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("SingleColumnConformanceRule", true)
  }

  get inputColumnAliasControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--inputColumnAlias`);
  }

  isCorrectlyConfigured(rule) {
    return this.hasValidInputColumn(rule.inputColumn)
      & this.hasValidInputColumnAlias(rule.inputColumnAlias);
  }

  hasValidInputColumnAlias(fieldValue) {
    return this.hasValidFlatColumn(fieldValue, "Input Column Alias", this.inputColumnAliasControl)
  }

  reset() {
    super.reset();
    this.resetInputColumn();
    this.resetValueState(this.inputColumnAliasControl);
  }

}

class SparkSessionConfConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("SparkSessionConfConformanceRule", false)
  }

  get sparkConfKeyControl() {
    return sap.ui.getCore().byId(`${this.ruleType}--sparkConfKey`);
  }

  isCorrectlyConfigured(rule) {
    return this.hasValidSparkConfKey(rule.sparkConfKey);
  }

  hasValidSparkConfKey(fieldValue) {
    return this.hasValidColumn(fieldValue, "Spark Config Key", this.sparkConfKeyControl);
  }

  reset() {
    super.reset();
    this.resetValueState(this.sparkConfKeyControl);
  }

}

class UppercaseConformanceRuleForm extends ConformanceRuleForm {

  constructor() {
    super("UppercaseConformanceRule", true)
  }

  isCorrectlyConfigured(rule) {
    return this.hasValidInputColumn(rule.inputColumn);
  }

  reset() {
    super.reset();
    this.resetInputColumn();
  }

}
